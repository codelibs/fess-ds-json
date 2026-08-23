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
import java.util.regex.Pattern;

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
import org.codelibs.fess.mylasta.direction.FessConfig;
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
 * <li>file_encoding - Character encoding for files (default: UTF-8)</li>
 * <li>format - Document shape: auto, jsonl or json (default: auto)</li>
 * <li>root_path - JSON Pointer selecting a nested array to read records from</li>
 * </ul>
 */
public class JsonDataStore extends AbstractDataStore {
    private static final Logger logger = LogManager.getLogger(JsonDataStore.class);

    /**
     * Fallback used when the configured encrypt-property pattern cannot be obtained from
     * {@link org.codelibs.fess.mylasta.direction.FessConfig}, matching the documented default
     * for {@code app.encrypt.property.pattern} in {@code fess_config.properties}.
     */
    protected static final String DEFAULT_ENCRYPT_PROPERTY_PATTERN = ".*password|.*key|.*token|.*secret";

    /**
     * Character encoding of the sources.
     *
     * <p>
     * The old camelCase spelling {@code fileEncoding} keeps working: DataStoreParams
     * resolves camelCase and snake_case keys to each other.
     * </p>
     */
    protected static final String FILE_ENCODING_PARAM = "file_encoding";

    /** Document shape: auto, jsonl or json. */
    protected static final String FORMAT_PARAM = "format";

    /** JSON Pointer selecting a nested array. */
    protected static final String ROOT_PATH_PARAM = "root_path";

    /**
     * How many consecutive record failures the token-stream path tolerates before this data
     * store gives up on a source.
     *
     * <p>
     * A line-oriented read cannot spin: every attempt consumes one line, so the read always
     * reaches end of file. A token stream can - a document truncated mid-object never
     * resynchronizes, and Jackson keeps reporting a fresh failure for every character it steps
     * over without ever reaching the end of the stream. This bound turns that into one warning
     * instead of an endless crawl, while still leaving room for the token stream to recover from
     * a run of garbage in the middle of an otherwise readable document.
     * </p>
     */
    protected static final int MAX_CONSECUTIVE_TOKEN_FAILURES = 100;

    /** Sentinel for "the previous record did not fail", distinct from every line number including -1. */
    private static final int NO_FAILING_LINE = Integer.MIN_VALUE;

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
        // Compiled once for the whole crawl, not once per record: a Pattern is looked up and
        // compiled from FessConfig here, then reused by createScriptParamMap for every source
        // and every record processSource reads.
        final Pattern encryptPropertyPattern = resolveEncryptPropertyPattern();
        final JsonRecordReader.Format format;
        try {
            // Resolved here, once for the whole crawl rather than once per source, and inside
            // guarded code: parseFormat rejects an unknown value with a DataStoreException, and
            // from processSource that would escape storeData with no failure record and abandon
            // every remaining source instead of reporting a plain configuration error.
            format = JsonRecordReader.parseFormat(paramMap.getAsString(FORMAT_PARAM));
        } catch (final DataStoreException e) {
            logger.warn("Invalid {} parameter.", FORMAT_PARAM, e);
            ComponentUtil.getComponent(FailureUrlService.class)
                    .store(dataConfig, e.getClass().getCanonicalName(), getName() + ":" + FORMAT_PARAM, e);
            return;
        }
        final String rootPath = paramMap.getAsString(ROOT_PATH_PARAM);
        if (StringUtil.isNotBlank(rootPath) && format == JsonRecordReader.Format.JSONL) {
            // Not an error, and not worth abandoning the crawl over, but the two parameters
            // contradict each other and only one of them can win. Say which, rather than leave
            // the user to work out from the results that their format was quietly dropped.
            logger.warn("{} is ignored because {} is set: a document reached through a JSON Pointer is read as a token stream, "
                    + "not line by line.", FORMAT_PARAM, ROOT_PATH_PARAM);
        }
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
            if (!processSource(dataConfig, callback, paramMap, scriptMap, defaultDataMap, source, fileEncoding, format, rootPath,
                    encryptPropertyPattern)) {
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
     * Resolves the pattern used to keep sensitive data store parameters out of the script
     * scope, from {@code app.encrypt.property.pattern}.
     *
     * <p>
     * This is a security filter, not a convenience default: if {@link ComponentUtil#getFessConfig()}
     * is unavailable for any reason - it returns {@code null}, the configured pattern is blank, or
     * the lookup itself throws (missing container, unbound component, ...) - this falls back to
     * {@link #DEFAULT_ENCRYPT_PROPERTY_PATTERN} rather than skip filtering. Failing open here would
     * be worse than failing closed.
     * </p>
     *
     * @return the compiled encrypt-property pattern
     */
    protected Pattern resolveEncryptPropertyPattern() {
        try {
            final FessConfig fessConfig = fetchFessConfig();
            if (fessConfig != null) {
                final String pattern = fessConfig.getAppEncryptPropertyPattern();
                if (pattern != null && !pattern.isBlank()) {
                    return Pattern.compile(pattern);
                }
            }
        } catch (final RuntimeException e) {
            // Covers a missing/unbound FessConfig component as well as a malformed configured
            // pattern (PatternSyntaxException extends RuntimeException) - either way, fall back
            // below instead of leaving sensitive parameters unfiltered.
            logger.warn("Failed to resolve the {} pattern; falling back to the default pattern.", FessConfig.APP_ENCRYPT_PROPERTY_PATTERN,
                    e);
        }
        return Pattern.compile(DEFAULT_ENCRYPT_PROPERTY_PATTERN);
    }

    /**
     * Fetches the current FessConfig component.
     *
     * <p>
     * Isolated into its own method purely as a test seam: it lets tests simulate every way
     * {@link ComponentUtil#getFessConfig()} can fail to supply a usable FessConfig - returning
     * {@code null}, or throwing - by overriding this one method, instead of needing a full
     * FessConfig stub that implements the rest of that (very large) interface.
     * </p>
     *
     * @return the FessConfig component, or {@code null} if none is available
     */
    protected FessConfig fetchFessConfig() {
        return ComponentUtil.getFessConfig();
    }

    /**
     * Builds the parameter map exposed to the field scripts, with sensitive values removed.
     *
     * <p>
     * Data store parameters are visible to scripts as top-level variables. Fess decrypts
     * parameters matching {@code app.encrypt.property.pattern} before handing them to the
     * plugin, so passing them through unchanged would let a script line such as
     * {@code leaked=crawler.web.auth.a.password} copy a credential straight into an indexed
     * field via {@code AbstractDataStore#convertValue}'s exact-key shortcut - no script engine
     * involved. Scripts are not sandboxed, so the filtering happens here, before the scope a
     * script can read is ever built.
     * </p>
     *
     * <p>
     * A matching key is kept present with a {@code null} value rather than dropped outright.
     * {@code convertValue} treats an absent key as "not a literal parameter reference" and falls
     * back to evaluating the template as a script through the configured script engine instead.
     * That fallback does not by itself lose the record: a raw parameter name such as
     * {@code crawler.web.auth.a.password} is valid Groovy (a property-access chain) - it fails at
     * runtime with {@code MissingPropertyException} because nothing binds a variable named
     * {@code crawler}, and {@code GroovyEngine#evaluate} catches exactly that, logs a WARN, and
     * returns {@code null}. Dropping the key was rejected anyway, on grounds independent of that
     * outcome: keeping it present-and-null never invokes the script engine for a blocked
     * parameter at all, so blocking a field costs no script compile and no WARN per record; it
     * does not depend on Groovy specifically, so a {@code scriptType} with no engine registered
     * for it - as in this project's own unit tests, which register no
     * {@link org.codelibs.fess.script.ScriptEngineFactory} - genuinely does fail the whole record
     * on the dropped-key fallback path; and it is the only shape under which "the record is
     * still indexed with the field simply absent" can be relied on regardless of which script
     * engine is configured. Keeping the key present with {@code containsKey() == true} and a
     * {@code null} value instead lets {@code convertValue}'s exact-match fast path apply and
     * return {@code null} directly. It also keeps this filter from fighting the override in
     * {@link #processSource}: a record field of the same name still replaces this {@code null}
     * afterwards, so record data keeps taking priority over a blocked parameter exactly as it
     * does over a visible one.
     * </p>
     *
     * <p>
     * A script that reaches a blocked parameter only through the script engine - for example by
     * concatenating it, as in {@code "'[' + access_token + ']'"} - still cannot recover the
     * secret, but the field does not simply come out empty either: Groovy's {@code +} stringifies
     * a {@code null} operand, so the result is the literal text {@code "[null]"}, not
     * {@code "[]"} and not an error. Whatever field a script builds that way indexes that literal
     * text.
     * </p>
     *
     * <p>
     * One key currently matches this pattern by coincidence, not by design:
     * {@link Constants#CRAWLER_STATS_KEY} ({@code "crawler.stats.key"}) ends in "key", so the
     * per-record {@link StatsKeyObject} that {@link #processSource} stores into {@code paramMap}
     * is filtered out of the script scope too. This map is built once per source, before that
     * source's record loop starts, while {@code paramMap} itself is mutated inside the loop - so
     * the key is simply absent (not merely {@code null}) for a source's first record, and
     * present-and-{@code null} with the <em>previous</em> source's stale {@code StatsKeyObject}
     * for every source read after the first. Nothing currently reads this key from a script, so
     * this has no observed effect, but it is not intentional filtering: if this constant's
     * spelling ever changes so it no longer matches {@code encryptPropertyPattern}, a script
     * would start seeing a stale {@code StatsKeyObject} from the previous source - so treat a
     * rename of that constant as a reason to re-examine this.
     * </p>
     *
     * @param paramMap the data store parameters
     * @param encryptPropertyPattern the pattern identifying sensitive parameter keys
     * @return the parameters a script may read, with sensitive values replaced by {@code null}
     */
    protected Map<String, Object> createScriptParamMap(final DataStoreParams paramMap, final Pattern encryptPropertyPattern) {
        final Map<String, Object> scriptParamMap = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : paramMap.asMap().entrySet()) {
            final String key = entry.getKey();
            scriptParamMap.put(key, encryptPropertyPattern.matcher(key).matches() ? null : entry.getValue());
        }
        return scriptParamMap;
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
     * @param format the document shape, already resolved from the parameters
     * @param rootPath a JSON Pointer selecting a nested array, may be {@code null}
     * @param encryptPropertyPattern the pattern identifying sensitive parameter keys, resolved once
     *            for the whole {@code storeData} call
     * @return {@code false} if crawling must stop, {@code true} to continue with the next source
     */
    protected boolean processSource(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final JsonSource source,
            final String fileEncoding, final JsonRecordReader.Format format, final String rootPath, final Pattern encryptPropertyPattern) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final String scriptType = getScriptType(paramMap);
        final long readInterval = getReadInterval(paramMap);
        // Built once for this source rather than once per record: resultMap.putAll(record) below
        // creates a fresh copy per record from this shared base, so a script's exact-key lookup
        // (AbstractDataStore#convertValue) never touches a parameter matching
        // encryptPropertyPattern. See createScriptParamMap's javadoc for why that matters.
        final Map<String, Object> scriptParamMap = createScriptParamMap(paramMap, encryptPropertyPattern);
        boolean aborted = false;

        logger.info("Loading {}", source.getName());
        try (JsonSource openSource = source;
                InputStream in = openSource.openStream();
                JsonRecordReader reader = new JsonRecordReader(in, fileEncoding, format, rootPath)) {
            int count = 0;
            int consecutiveFailures = 0;
            int lastFailingLine = NO_FAILING_LINE;
            while (alive && reader.hasNext()) {
                // reader.next() is attempted here, before the stats key exists, because
                // getCurrentLineNumber() only reflects the record this call just attempted -
                // JsonRecordReader records the line a record starts on right before parsing it,
                // so reading the line number any earlier would still report the PREVIOUS
                // record's line. next() sets that line even when it throws (the malformed-JSON
                // case), so capturing the failure here and re-throwing it below still reports
                // the real line the broken record starts on rather than the record ordinal. The
                // one exception is a token stream that never finds another record at all: it
                // reaches no new line, so the failure is reported against the last one it read.
                Map<String, Object> record = null;
                Throwable readFailure = null;
                try {
                    record = reader.next();
                } catch (final Throwable t) {
                    readFailure = t;
                }
                final int lineNumber = reader.getCurrentLineNumber();

                if (readFailure == null) {
                    consecutiveFailures = 0;
                    lastFailingLine = NO_FAILING_LINE;
                } else {
                    consecutiveFailures++;
                    final boolean sameRegion = lineNumber == lastFailingLine;
                    lastFailingLine = lineNumber;
                    if (sameRegion) {
                        // The token stream is still picking its way through the same unparseable
                        // stretch it already reported - it steps over one character at a time and
                        // raises a fresh failure for each. Reporting every one of those would
                        // multiply a single bad region into a pile of identical failure records
                        // against the same URL and flood the log. It still counts towards the
                        // give-up bound below, whose warning carries the real total.
                        if (!reader.isLineOriented() && consecutiveFailures >= MAX_CONSECUTIVE_TOKEN_FAILURES) {
                            warnGaveUp(source, consecutiveFailures, reader);
                            break;
                        }
                        continue;
                    }
                }

                count++;
                final StatsKeyObject statsKey = new StatsKeyObject(source.getName() + "@" + (lineNumber >= 0 ? lineNumber : count));
                paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
                final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
                try {
                    crawlerStatsHelper.begin(statsKey);
                    if (readFailure != null) {
                        throw readFailure;
                    }
                    final Map<String, Object> resultMap = new LinkedHashMap<>(scriptParamMap);
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
                // A line-oriented read always moves on to the next line, so a bad record only ever
                // costs that record. A token stream may instead be stuck on a remainder it can
                // never resynchronize with - a document truncated mid-object keeps failing without
                // ever reaching end of stream - so give up on this source once it has produced
                // nothing but failures for long enough.
                if (readFailure != null && !reader.isLineOriented() && consecutiveFailures >= MAX_CONSECUTIVE_TOKEN_FAILURES) {
                    warnGaveUp(source, consecutiveFailures, reader);
                    break;
                }
                if (readInterval > 0) {
                    sleep(readInterval);
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
     * Logs the one warning that a source was abandoned because its token stream stopped making
     * progress.
     *
     * <p>
     * The line number is approximate: these failures come from the reader looking for the next
     * record and never finding one, so {@link JsonRecordReader#getCurrentLineNumber()} still
     * reports where the last record it read successfully began.
     * </p>
     *
     * @param source the source being abandoned
     * @param consecutiveFailures how many failures in a row it produced, reported or suppressed
     * @param reader the reader, for its approximate position
     */
    private void warnGaveUp(final JsonSource source, final int consecutiveFailures, final JsonRecordReader reader) {
        logger.warn("Gave up on {} after {} consecutive parse failures near line {}: the document does not resynchronize. "
                + "The rest of it was not read.", source.getName(), consecutiveFailures, reader.getCurrentLineNumber());
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
