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

import static org.codelibs.core.stream.StreamUtil.stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON Data Store implementation for Fess that processes JSON and JSONL files.
 * This data store extends AbstractDataStore to provide functionality for crawling
 * and indexing JSON and JSONL files from the filesystem.
 *
 * <p>Supported file formats:</p>
 * <ul>
 * <li>.json - Standard JSON files</li>
 * <li>.jsonl - JSON Lines format (one JSON object per line)</li>
 * </ul>
 *
 * <p>Configuration parameters:</p>
 * <ul>
 * <li>files - Comma-separated list of file paths to process</li>
 * <li>directories - Comma-separated list of directory paths to scan</li>
 * <li>fileEncoding - Character encoding for files (default: UTF-8)</li>
 * </ul>
 */
public class JsonDataStore extends AbstractDataStore {
    private static final Logger logger = LogManager.getLogger(JsonDataStore.class);

    private static final String FILE_ENCODING_PARAM = "fileEncoding";

    private static final String FILES_PARAM = "files";

    private static final String DIRS_PARAM = "directories";

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
        final List<File> fileList = getFileList(paramMap);

        if (fileList.isEmpty()) {
            logger.warn("No files to process");
            return;
        }

        for (final File file : fileList) {
            if (!alive) {
                logger.info("Stopped crawling: {}", file.getAbsolutePath());
                break;
            }
            if (!processFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, file, fileEncoding)) {
                // The data store was asked to abort on this record; stop reading further files.
                break;
            }
        }
    }

    private List<File> getFileList(final DataStoreParams paramMap) {
        String value = paramMap.getAsString(FILES_PARAM);
        final List<File> fileList = new ArrayList<>();
        if (StringUtil.isBlank(value)) {
            value = paramMap.getAsString(DIRS_PARAM);
            if (StringUtil.isBlank(value)) {
                throw new DataStoreException(FILES_PARAM + " and " + DIRS_PARAM + " are blank.");
            }
            logger.info("{}={}", DIRS_PARAM, value);
            final String[] values = splitPaths(value);
            for (final String path : values) {
                final File dir = new File(path);
                if (dir.isDirectory()) {
                    stream(dir.listFiles()).of(stream -> stream.filter(f -> isDesiredFile(f.getParentFile(), f.getName()))
                            .sorted(Comparator.comparingLong(File::lastModified))
                            .forEach(fileList::add));
                } else {
                    logger.warn("{} is not a directory.", path);
                }
            }
        } else {
            logger.info("{}={}", FILES_PARAM, value);
            final String[] values = splitPaths(value);
            for (final String path : values) {
                final File file = new File(path);
                if (!file.exists()) {
                    logger.warn("{} does not exist.", path);
                } else if (!file.isFile()) {
                    logger.warn("{} is not a file.", path);
                } else if (!isDesiredFile(file.getParentFile(), file.getName())) {
                    logger.warn("{} is skipped because its suffix is not one of {}.", path, Arrays.toString(fileSuffixes));
                } else {
                    fileList.add(file);
                }
            }
        }
        if (fileList.isEmpty() && logger.isDebugEnabled()) {
            logger.debug("No files in {}", value);
        }
        return fileList;
    }

    /**
     * Splits a comma-separated parameter value and drops blank elements.
     *
     * @param value comma-separated value
     * @return trimmed, non-blank elements
     */
    private String[] splitPaths(final String value) {
        return stream(value.split(",")).get(stream -> stream.map(String::trim).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
    }

    private boolean isDesiredFile(final File parentFile, final String filename) {
        final String name = filename.toLowerCase(Locale.ROOT);
        for (final String suffix : fileSuffixes) {
            if (name.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private String getFileEncoding(final DataStoreParams paramMap) {
        return paramMap.getAsString(FILE_ENCODING_PARAM, Constants.UTF_8);
    }

    /**
     * Processes a single file and stores each record through the callback.
     *
     * @param dataConfig the data store configuration
     * @param callback the index update callback
     * @param paramMap the data store parameters
     * @param scriptMap the script mappings for field conversion
     * @param defaultDataMap the default data values
     * @param file the file to process
     * @param fileEncoding the character encoding of the file
     * @return {@code false} if crawling must stop, {@code true} to continue with the next file
     */
    private boolean processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final File file, final String fileEncoding) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final ObjectMapper objectMapper = new ObjectMapper();

        final String scriptType = getScriptType(paramMap);
        logger.info("Loading {}", file.getAbsolutePath());
        boolean aborted = false;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), fileEncoding))) {
            int count = 0;
            for (String line; alive && (line = br.readLine()) != null;) {
                count++;
                if (count == 1) {
                    line = stripBom(line);
                }
                if (StringUtil.isBlank(line)) {
                    // A blank line is not a record. Skipping it silently keeps it out of
                    // the failure URL list, matching how CsvDataStore treats empty rows.
                    continue;
                }
                final StatsKeyObject statsKey = new StatsKeyObject(file.getAbsolutePath() + "@" + count);
                paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
                final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
                try {
                    crawlerStatsHelper.begin(statsKey);
                    final Map<String, Object> source = objectMapper.readValue(line, new TypeReference<Map<String, Object>>() {
                    });
                    final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());

                    resultMap.putAll(source);

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
                    logger.warn("Crawling Access Exception at : {}", dataMap, e);

                    Throwable target = e;
                    if (target instanceof final MultipleCrawlingAccessException ex) {
                        final Throwable[] causes = ex.getCauses();
                        if (causes.length > 0) {
                            target = causes[causes.length - 1];
                        }
                    }

                    String errorName;
                    final Throwable cause = target.getCause();
                    if (cause != null) {
                        errorName = cause.getClass().getCanonicalName();
                    } else {
                        errorName = target.getClass().getCanonicalName();
                    }

                    String url = statsKey.getId();
                    if (target instanceof final DataStoreCrawlingException dce) {
                        if (dce.getUrl() != null) {
                            url = dce.getUrl();
                        }
                        if (dce.aborted()) {
                            aborted = true;
                        }
                    }

                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(dataConfig, errorName, url, target);
                    crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
                } catch (final Throwable t) {
                    logger.warn("Crawling Access Exception at : {}", dataMap, t);
                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), statsKey.getId(), t);
                    crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
                } finally {
                    // Call done() exactly once per record regardless of the outcome above, then break
                    // out of the loop afterwards if the exception handler asked us to abort - avoids
                    // the harmless-but-confusing double done() call that calling it again before break
                    // would cause (CrawlerStatsHelper#done treats a second call as a silent no-op).
                    crawlerStatsHelper.done(statsKey);
                }
                if (aborted) {
                    break;
                }
            }
        } catch (final FileNotFoundException e) {
            logger.warn("Source file {} does not exist.", file, e);
            recordFileFailure(dataConfig, file, e);
        } catch (final IOException e) {
            logger.warn("IO Error occurred while reading source file {}.", file, e);
            recordFileFailure(dataConfig, file, e);
        }
        return !aborted;
    }

    /**
     * Records a file-level failure so that a crawl which could not read one of its inputs
     * is not reported as fully successful.
     *
     * @param dataConfig the data configuration being crawled
     * @param file the file that could not be read
     * @param e the cause
     */
    private void recordFileFailure(final DataConfig dataConfig, final File file, final Throwable e) {
        final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
        failureUrlService.store(dataConfig, e.getClass().getCanonicalName(), file.getAbsolutePath(), e);
    }

    /** Zero-width no-break space, i.e. the character a UTF-8 BOM decodes to. */
    /** Zero-width no-break space, i.e. the character a UTF-8 BOM decodes to. */
    private static final char BOM_CHAR = '\uFEFF';

    /**
     * Removes a leading byte order mark from the first line of a file.
     *
     * <p>
     * {@link InputStreamReader} decodes a UTF-8 BOM to U+FEFF and hands it to the caller
     * rather than discarding it, which makes the first JSON object of a BOM-prefixed file
     * unparseable.
     * </p>
     *
     * @param line the line to clean
     * @return the line without a leading BOM
     */
    private String stripBom(final String line) {
        if (!line.isEmpty() && line.charAt(0) == BOM_CHAR) {
            return line.substring(1);
        }
        return line;
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
