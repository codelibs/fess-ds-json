/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

/**
 * JSON Data Store implementation for Fess that processes JSON and JSONL files.
 * This data store extends AbstractDataStore to provide functionality for crawling
 * and indexing JSON and JSONL files from the filesystem.
 *
 * <p>Supported file formats:</p>
 * <ul>
 * <li>.json - Standard JSON files, either a single object or an array of objects</li>
 * <li>.jsonl - JSON Lines format (one JSON object per line)</li>
 * </ul>
 *
 * <p>Configuration parameters:</p>
 * <ul>
 * <li>files - Comma-separated list of file paths to process</li>
 * <li>directories - Comma-separated list of directory paths to scan</li>
 * <li>fileEncoding - Character encoding for files (default: UTF-8)</li>
 * <li>format - Document shape: auto, jsonl or json (default: auto)</li>
 * <li>root_path - JSON Pointer selecting a nested array to read records from</li>
 * </ul>
 */
public class JsonDataStore extends AbstractDataStore {
    private static final Logger logger = LogManager.getLogger(JsonDataStore.class);

    private static final String FILE_ENCODING_PARAM = "fileEncoding";

    /** Document shape: auto, jsonl or json. */
    protected static final String FORMAT_PARAM = "format";

    /** JSON Pointer selecting a nested array. */
    protected static final String ROOT_PATH_PARAM = "root_path";

    private String[] fileSuffixes = { ".json", ".jsonl" };

    /**
     * Default constructor for JsonDataStore.
     * Initializes the data store with default file suffixes for JSON and JSONL files.
     */
    public JsonDataStore() {
        super();
    }

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final String fileEncoding = getFileEncoding(paramMap);
        final List<JsonSource> sourceList = createSourceResolver().resolve(paramMap);

        if (sourceList.isEmpty()) {
            logger.warn("No sources to process");
            return;
        }

        for (final JsonSource source : sourceList) {
            if (!alive) {
                logger.info("Stopped crawling: {}", source.getName());
                break;
            }
            if (!processSource(dataConfig, callback, paramMap, scriptMap, defaultDataMap, source, fileEncoding)) {
                // The data store was asked to abort on this record; stop reading further sources.
                break;
            }
        }
    }

    /**
     * Creates the resolver that turns parameters into sources.
     *
     * @return a resolver seeded with this data store's default file suffixes
     */
    protected JsonSourceResolver createSourceResolver() {
        return new JsonSourceResolver(fileSuffixes);
    }

    private String getFileEncoding(final DataStoreParams paramMap) {
        return paramMap.getAsString(FILE_ENCODING_PARAM, Constants.UTF_8);
    }

    /**
     * Reads one source and stores each of its records.
     *
     * @param dataConfig the data configuration being crawled
     * @param callback the index update callback
     * @param paramMap the data store parameters
     * @param scriptMap the field-to-script mapping
     * @param defaultDataMap the base document
     * @param source the source to read
     * @param fileEncoding the character encoding
     * @return {@code false} if crawling must stop, {@code true} to continue with the next source
     */
    protected boolean processSource(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final JsonSource source,
            final String fileEncoding) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final String scriptType = getScriptType(paramMap);
        final JsonRecordReader.Format format = JsonRecordReader.parseFormat(paramMap.getAsString(FORMAT_PARAM));
        final String rootPath = paramMap.getAsString(ROOT_PATH_PARAM);
        final long readInterval = getReadInterval(paramMap);
        boolean aborted = false;

        logger.info("Loading {}", source.getName());
        try (JsonSource openSource = source;
                InputStream in = openSource.openStream();
                JsonRecordReader reader = new JsonRecordReader(in, fileEncoding, format, rootPath)) {
            int count = 0;
            while (alive && reader.hasNext()) {
                count++;
                // reader.next() is attempted here, before the stats key exists, because
                // getCurrentLineNumber() only reflects the record this call just attempted -
                // JsonRecordReader records the line a record starts on right before parsing it,
                // so reading the line number any earlier would still report the PREVIOUS
                // record's line. next() sets that line even when it throws (the malformed-JSON
                // case), so capturing the failure here and re-throwing it below still reports
                // the real line the broken record starts on rather than the record ordinal.
                Map<String, Object> record = null;
                Throwable readFailure = null;
                try {
                    record = reader.next();
                } catch (final Throwable t) {
                    readFailure = t;
                }
                final int lineNumber = reader.getCurrentLineNumber();
                final StatsKeyObject statsKey = new StatsKeyObject(source.getName() + "@" + (lineNumber >= 0 ? lineNumber : count));
                paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
                final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
                try {
                    crawlerStatsHelper.begin(statsKey);
                    if (readFailure != null) {
                        throw readFailure;
                    }
                    final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
                    resultMap.putAll(record);

                    crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

                    for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                        final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                        if (convertValue != null) {
                            dataMap.put(entry.getKey(), convertValue);
                        }
                    }

                    crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

                    if (dataMap.get("url") instanceof final String statsUrl) {
                        statsKey.setUrl(statsUrl);
                    }

                    callback.store(paramMap, dataMap);
                    crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
                } catch (final CrawlingAccessException e) {
                    aborted = handleCrawlingAccessException(dataConfig, crawlerStatsHelper, statsKey, dataMap, e);
                    if (aborted) {
                        crawlerStatsHelper.done(statsKey);
                        break;
                    }
                } catch (final Throwable t) {
                    logger.warn("Crawling Access Exception at : {}", dataMap, t);
                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), statsKey.getId(), t);
                    crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
                } finally {
                    crawlerStatsHelper.done(statsKey);
                }
                if (readInterval > 0) {
                    sleep(readInterval);
                }
                if (readFailure != null) {
                    // Unlike a line-based format, a JSON stream cannot resynchronize after a
                    // malformed record: the underlying parser is left mid-token, and every
                    // further hasNext()/next() call only advances one invalid character at a
                    // time, throwing again rather than finding the next well-formed record (or
                    // even reliably detecting end of stream). The failure above already reports
                    // this record; stop reading the rest of this source instead of repeating it.
                    break;
                }
            }
        } catch (final IOException e) {
            logger.warn("Failed to read {}.", source.getName(), e);
            recordSourceFailure(dataConfig, source, e);
        } catch (final DataStoreException e) {
            logger.warn("Failed to parse {}.", source.getName(), e);
            recordSourceFailure(dataConfig, source, e);
        }
        return !aborted;
    }

    /**
     * Records a crawling access failure and reports whether crawling must stop.
     *
     * @param dataConfig the data configuration being crawled
     * @param crawlerStatsHelper the statistics helper
     * @param statsKey the statistics key for this record
     * @param dataMap the partially built document, for logging
     * @param e the failure
     * @return {@code true} if the exception asked the crawl to abort
     */
    private boolean handleCrawlingAccessException(final DataConfig dataConfig, final CrawlerStatsHelper crawlerStatsHelper,
            final StatsKeyObject statsKey, final Map<String, Object> dataMap, final CrawlingAccessException e) {
        logger.warn("Crawling Access Exception at : {}", dataMap, e);

        Throwable target = e;
        if (target instanceof final MultipleCrawlingAccessException ex) {
            final Throwable[] causes = ex.getCauses();
            if (causes.length > 0) {
                target = causes[causes.length - 1];
            }
        }

        final Throwable cause = target.getCause();
        final String errorName = cause != null ? cause.getClass().getCanonicalName() : target.getClass().getCanonicalName();

        String url = statsKey.getId();
        boolean aborted = false;
        if (target instanceof final DataStoreCrawlingException dce) {
            if (dce.getUrl() != null) {
                url = dce.getUrl();
            }
            aborted = dce.aborted();
        }

        ComponentUtil.getComponent(FailureUrlService.class).store(dataConfig, errorName, url, target);
        crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        return aborted;
    }

    /**
     * Records a source-level failure so that a crawl which could not read one of its
     * inputs is not reported as fully successful.
     *
     * @param dataConfig the data configuration being crawled
     * @param source the source that could not be read
     * @param e the cause
     */
    private void recordSourceFailure(final DataConfig dataConfig, final JsonSource source, final Throwable e) {
        ComponentUtil.getComponent(FailureUrlService.class).store(dataConfig, e.getClass().getCanonicalName(), source.getName(), e);
    }

    /**
     * Sets the file suffixes that this data store will process.
     *
     * @param fileSuffixes Array of file suffixes (e.g., ".json", ".jsonl")
     */
    public void setFileSuffixes(final String[] fileSuffixes) {
        this.fileSuffixes = fileSuffixes;
    }
}
