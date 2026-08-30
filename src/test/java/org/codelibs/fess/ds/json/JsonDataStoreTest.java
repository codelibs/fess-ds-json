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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.CrawlingConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.opensearch.config.exentity.FailureUrl;
import org.codelibs.fess.script.ScriptEngineFactory;
import org.codelibs.fess.script.javascript.JavaScriptEngine;
import org.codelibs.fess.util.ComponentUtil;

/**
 * Comprehensive unit tests for JsonDataStore class.
 * Tests cover encoding handling, JSON/JSONL processing through the full storeData
 * pipeline, and error scenarios.
 *
 * <p>
 * Source discovery (file/directory resolution, suffix filtering, sorting) is covered by
 * {@link JsonSourceResolverTest} instead of here; this class exercises storeData end to
 * end, including the parts JsonSourceResolverTest cannot reach on its own.
 * </p>
 *
 * Note: setUp registers SystemHelper, an initialized CrawlerStatsHelper and a recording
 * FailureUrlService stub in the DI container so that storeData tests exercise the full
 * pipeline (including error recording) rather than mocking or catching exceptions.
 */
public class JsonDataStoreTest extends UnitDsTestCase {

    /**
     * A pretty-printed object wrapping an array of records, the shape whose AUTO handling the
     * column-zero question turned on. Shared by the root_path test and the without-root_path
     * test so both are demonstrably talking about the same document.
     */
    private static final String PRETTY_WRAPPER = "{\n  \"meta\": 1,\n  \"items\": [\n    {\"url\":\"http://example.com/1\"},\n"
            + "    {\"url\":\"http://example.com/2\"}\n  ]\n}\n";

    public JsonDataStore dataStore;

    /** Failures recorded through FailureUrlService, held as "<errorName> @ <url>". */
    public final List<String> failureUrls = new ArrayList<>();

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new JsonDataStore();
        failureUrls.clear();

        // storeData uses CrawlerStatsHelper, which in turn uses SystemHelper. Register initialized
        // instances of both so that the tests run the whole pipeline.
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");

        // The real failure store needs OpenSearch, so substitute a no-op that only remembers what
        // it was asked to record. Registered under the canonical name so that
        // ComponentUtil.getComponent(Class) resolves it.
        ComponentUtil.register(new FailureUrlService() {
            @Override
            public FailureUrl store(final CrawlingConfig crawlingConfig, final String errorName, final String url, final Throwable e) {
                failureUrls.add(errorName + " @ " + url);
                return null;
            }
        }, FailureUrlService.class.getCanonicalName());

        // Register the real JavaScriptEngine: AbstractDataStore#convertValue comes through it
        // whenever a scriptMap template is not an exact key of resultMap - a concatenation, for
        // example. Constructing it directly rather than through the DI container means
        // @PostConstruct init() never runs, but the script cache evaluate() needs is built by the
        // constructor's own buildScriptCache(). register() binds it under its own name
        // (Constants.DEFAULT_SCRIPT) and its aliases.
        ComponentUtil.register(new ScriptEngineFactory(), "scriptEngineFactory");
        new JavaScriptEngine().register();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    /**
     * Test that getName returns the correct class simple name.
     */
    @Test
    public void test_getName() {
        assertEquals("JsonDataStore", dataStore.getName());
    }

    /**
     * Test that a DI-supplied setFileSuffixes reaches the resolver.
     */
    @Test
    public void test_setFileSuffixes_customSuffixes() throws Exception {
        final Path dir = Files.createTempDirectory("suffix");
        Files.writeString(dir.resolve("a.ndjson"), "{\"url\":\"http://example.com/1\"}\n");
        Files.writeString(dir.resolve("b.json"), "{\"url\":\"http://example.com/2\"}\n");

        dataStore.setFileSuffixes(new String[] { ".ndjson" });

        final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
        final DataStoreParams params = new DataStoreParams();
        params.put("directories", dir.toString());
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");

        dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

        assertEquals("only the .ndjson file is processed", 1, callback.getDataMapList().size());
        assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));
    }

    /**
     * Test getFileEncoding with default UTF-8 encoding.
     */
    @Test
    public void test_getFileEncoding_default() throws Exception {
        DataStoreParams params = new DataStoreParams();
        String encoding = invokeMethod(dataStore, "getFileEncoding", params);
        assertEquals(Constants.UTF_8, encoding);
    }

    /**
     * Test getFileEncoding with custom encoding specified.
     */
    @Test
    public void test_getFileEncoding_custom() throws Exception {
        DataStoreParams params = new DataStoreParams();
        params.put("fileEncoding", "ISO-8859-1");
        String encoding = invokeMethod(dataStore, "getFileEncoding", params);
        assertEquals("ISO-8859-1", encoding);
    }

    /**
     * Test that the old fileEncoding spelling still works, through ParamMap's automatic
     * conversion, and that the internal parameter name is now file_encoding. ParamMap answers
     * either spelling with the same value, so the rename itself can only be observed from outside
     * by the reflective check below.
     */
    @Test
    public void test_getFileEncoding_legacyCamelCaseKey() throws Exception {
        final java.lang.reflect.Field field = JsonDataStore.class.getDeclaredField("FILE_ENCODING_PARAM");
        field.setAccessible(true);
        assertEquals("the parameter is renamed to snake_case", "file_encoding", field.get(null));

        final DataStoreParams params = new DataStoreParams();
        params.put("fileEncoding", "Shift_JIS");

        final String encoding = invokeMethod(dataStore, "getFileEncoding", params);

        assertEquals("Shift_JIS", encoding);
    }

    /**
     * Test that the new file_encoding spelling works on its own; the mirror image of the test
     * above, which tries only the old one.
     */
    @Test
    public void test_getFileEncoding_canonicalSnakeCaseKey() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put("file_encoding", "Shift_JIS");

        final String encoding = invokeMethod(dataStore, "getFileEncoding", params);

        assertEquals("Shift_JIS", encoding);
    }

    /**
     * Test storeData with empty file list.
     */
    @Test
    public void test_storeData_emptyFileList() throws Exception {
        DataConfig dataConfig = new DataConfig();
        TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
        DataStoreParams params = new DataStoreParams();
        Map<String, String> scriptMap = new HashMap<>();
        Map<String, Object> defaultDataMap = new HashMap<>();

        // Set invalid path to ensure empty file list
        params.put("files", "/nonexistent/path/test.json");

        // This should log a warning and return without processing
        dataStore.storeData(dataConfig, callback, params, scriptMap, defaultDataMap);

        // The callback must not receive anything when there is nothing to process.
        assertTrue("callback should receive no records for an empty file list", callback.getDataMapList().isEmpty());
    }

    /**
     * Test that storeData hands every record of a JSON/JSONL file to the callback.
     */
    @Test
    public void test_storeData_withValidFiles() throws Exception {
        final Path tempDir = Files.createTempDirectory("jsontest");
        final Path jsonFile = tempDir.resolve("a.json");
        final Path jsonlFile = tempDir.resolve("b.jsonl");

        try {
            Files.writeString(jsonFile, "{\"id\":\"123\",\"title\":\"Test\",\"url\":\"http://example.com/1\"}\n");
            Files.writeString(jsonlFile, "{\"id\":\"1\",\"title\":\"First\",\"url\":\"http://example.com/2\"}\n"
                    + "{\"id\":\"2\",\"title\":\"Second\",\"url\":\"http://example.com/3\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("directories", tempDir.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");
            scriptMap.put("title", "title");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            final List<Map<String, Object>> dataMapList = callback.getDataMapList();
            assertEquals("1 record from .json + 2 records from .jsonl", 3, dataMapList.size());
            assertEquals("no failures should be recorded", 0, failureUrls.size());

            // Processing order between files is not guaranteed (it depends on filesystem
            // mtime resolution), so look up each record by its unique url instead of
            // asserting on list position.
            final Map<String, Object> firstJsonRecord =
                    dataMapList.stream().filter(m -> "http://example.com/1".equals(m.get("url"))).findFirst().orElse(null);
            assertNotNull("missing record for http://example.com/1", firstJsonRecord);
            assertEquals("Test", firstJsonRecord.get("title"));

        } finally {
            Files.deleteIfExists(jsonFile);
            Files.deleteIfExists(jsonlFile);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * Test that a file whose suffix is not accepted is not processed.
     */
    @Test
    public void test_storeData_fileFiltering() throws Exception {
        final Path tempDir = Files.createTempDirectory("jsontest");
        final Path jsonFile = tempDir.resolve("a.json");
        final Path txtFile = tempDir.resolve("b.txt");

        try {
            Files.writeString(jsonFile, "{\"url\":\"http://example.com/1\"}\n");
            Files.writeString(txtFile, "{\"url\":\"http://example.com/2\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("directories", tempDir.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("only the .json file is processed", 1, callback.getDataMapList().size());
            assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));

        } finally {
            Files.deleteIfExists(jsonFile);
            Files.deleteIfExists(txtFile);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * Test that empty and blank lines are skipped rather than recorded as failures.
     */
    @Test
    public void test_storeData_skipsBlankLines() throws Exception {
        final Path file = Files.createTempFile("blank", ".jsonl");

        try {
            Files.writeString(file,
                    "{\"url\":\"http://example.com/1\"}\n" + "\n" + "   \n" + "{\"url\":\"http://example.com/2\"}\n" + "\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("two records are stored", 2, callback.getDataMapList().size());
            assertEquals("blank lines are not recorded as failures: " + failureUrls, 0, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that the first record of a file carrying a UTF-8 BOM is readable.
     */
    @Test
    public void test_storeData_stripsUtf8Bom() throws Exception {
        final Path file = Files.createTempFile("bom", ".jsonl");

        try {
            final byte[] bom = { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };
            final byte[] body = "{\"url\":\"http://example.com/1\"}\n{\"url\":\"http://example.com/2\"}\n".getBytes(StandardCharsets.UTF_8);
            final byte[] content = new byte[bom.length + body.length];
            System.arraycopy(bom, 0, content, 0, bom.length);
            System.arraycopy(body, 0, content, bom.length, body.length);
            Files.write(file, content);

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("both records are stored: " + failureUrls, 2, callback.getDataMapList().size());
            assertEquals("no failures", 0, failureUrls.size());
            assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * BOM-only first line followed by real records should result in no failures.
     *
     * <p>
     * This test originally pinned that BOM stripping ran before an explicit blank-line
     * check, i.e. that a line containing only a BOM was treated as blank and skipped. The
     * streaming reader strips the BOM once at stream open, and Jackson's tokenizer simply
     * skips whitespace between values, so that ordered pair of checks no longer exists as a
     * code path to pin. What the test still verifies, and what still matters, is the
     * observable behaviour: a BOM-prefixed file with a leading blank line indexes both real
     * records with no failures.
     * </p>
     */
    @Test
    public void test_storeData_bomOnlyFirstLineThenRecords() throws Exception {
        final Path file = Files.createTempFile("bomonly", ".jsonl");

        try {
            final byte[] bom = { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };
            final byte[] body =
                    "\n{\"url\":\"http://example.com/1\"}\n{\"url\":\"http://example.com/2\"}\n".getBytes(StandardCharsets.UTF_8);
            final byte[] content = new byte[bom.length + body.length];
            System.arraycopy(bom, 0, content, 0, bom.length);
            System.arraycopy(body, 0, content, bom.length, body.length);
            Files.write(file, content);

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("both records stored despite BOM-only first line", 2, callback.getDataMapList().size());
            assertEquals("no failures when BOM stripped before blank check: " + failureUrls, 0, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that alive is false after stop() and that no record is processed.
     */
    @Test
    public void test_storeData_stopsWhenNotAlive() throws Exception {
        final Path file = Files.createTempFile("stop", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n{\"url\":\"http://example.com/2\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.stop();
            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("nothing is processed after stop()", 0, callback.getDataMapList().size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that alive is checked in processSource's record loop as well as in the outer file loop.
     * stop() is called right after the first record is stored, and the second record is shown to
     * be unprocessed by its content (the first record's url) and not only by the count: a count
     * alone would still pass if the inner check were lost and the second record survived.
     */
    @Test
    public void test_storeData_stopsMidRecordLoop() throws Exception {
        final Path file = Files.createTempFile("stopmid", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n{\"url\":\"http://example.com/2\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback() {
                @Override
                public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                    super.store(paramMap, dataMap);
                    // Stop mid-crawl, from inside the very callback the record loop drives.
                    dataStore.stop();
                }
            };
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            final List<Map<String, Object>> dataMapList = callback.getDataMapList();
            assertEquals("only the first record is processed before stop() takes effect", 1, dataMapList.size());
            assertEquals("the surviving record is the first one", "http://example.com/1", dataMapList.get(0).get("url"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that a file which cannot be read is recorded as a failure.
     */
    @Test
    public void test_storeData_recordsUnreadableFile() throws Exception {
        final Path dir = Files.createTempDirectory("unreadable");
        final Path good = dir.resolve("good.jsonl");
        final Path bad = dir.resolve("bad.jsonl");

        try {
            Files.writeString(good, "{\"url\":\"http://example.com/1\"}\n");
            Files.writeString(bad, "{\"url\":\"http://example.com/2\"}\n");
            assertTrue("the file must become unreadable", bad.toFile().setReadable(false, false));
            // setReadable(false, ...) is a silent no-op for the file's owner when running as
            // root (e.g. inside a root Docker container), so self-skip rather than go red.
            Assumptions.assumeTrue(!Files.isReadable(bad), "cannot make file unreadable (running as root?)");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", good + "," + bad);
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("the readable file is still processed", 1, callback.getDataMapList().size());
            assertEquals("the unreadable file is recorded as a failure", 1, failureUrls.size());
            assertTrue("failure refers to the unreadable file: " + failureUrls, failureUrls.get(0).contains(bad.toString()));
        } finally {
            bad.toFile().setReadable(true, false);
            Files.deleteIfExists(good);
            Files.deleteIfExists(bad);
            Files.deleteIfExists(dir);
        }
    }

    /**
     * Test that a DataStoreCrawlingException(url, message, cause, true) thrown from store() breaks
     * out of both processSource's record loop and the source loop. The first file holds two
     * records and the second one, and the callback aborts on the first store(), so the second
     * file is never opened and exactly one record is attempted. This also pins where the failure
     * record's parts come from: errorName from the cause (IllegalStateException) rather than from
     * the exception itself, and url from DataStoreCrawlingException#getUrl() rather than from the
     * StatsKeyObject id.
     */
    @Test
    public void test_storeData_abortsOnDataStoreCrawlingException() throws Exception {
        final Path dir = Files.createTempDirectory("abort");
        final Path file1 = dir.resolve("a.jsonl");
        final Path file2 = dir.resolve("b.jsonl");

        try {
            Files.writeString(file1, "{\"url\":\"http://example.com/1\"}\n{\"url\":\"http://example.com/2\"}\n");
            // Ensure file1 sorts before file2 by last modified time.
            Thread.sleep(100);
            Files.writeString(file2, "{\"url\":\"http://example.com/3\"}\n");

            final List<Map<String, Object>> attempted = new ArrayList<>();
            final IndexUpdateCallback callback = new IndexUpdateCallback() {
                @Override
                public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                    attempted.add(new HashMap<>(dataMap));
                    throw new DataStoreCrawlingException("http://example.com/1", "boom", new IllegalStateException("boom"), true);
                }

                @Override
                public long getDocumentSize() {
                    return attempted.size();
                }

                @Override
                public long getExecuteTime() {
                    return 0;
                }

                @Override
                public void commit() {
                    // nothing to do
                }
            };

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("only the first record is attempted: the record loop breaks on abort and the source loop "
                    + "breaks too, so file2's record is never reached: " + attempted, 1, attempted.size());
            assertEquals("exactly one failure is recorded: " + failureUrls, 1, failureUrls.size());
            assertEquals(
                    "errorName must come from the exception's cause (IllegalStateException), not the "
                            + "exception itself, and url must come from DataStoreCrawlingException#getUrl(), " + "not the stats key id",
                    "java.lang.IllegalStateException @ http://example.com/1", failureUrls.get(0));
        } finally {
            Files.deleteIfExists(file1);
            Files.deleteIfExists(file2);
            Files.deleteIfExists(dir);
        }
    }

    /**
     * Test that line numbers keep following the real lines of the file across a blank line. The
     * first line is a good record, the second is blank and the third is malformed JSON, and the
     * recorded failure id must end in "@3" rather than "@2". This is what requires the line number
     * from JsonRecordReader#getCurrentLineNumber() instead of a record counter; with a counter the
     * failure no longer points at the line an editor shows.
     */
    @Test
    public void test_storeData_lineNumberTracksRealLinesAcrossBlank() throws Exception {
        final Path file = Files.createTempFile("linenum", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n" + "\n" + "{not valid json\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("only the valid first line is stored", 1, callback.getDataMapList().size());
            assertEquals("exactly one failure is recorded for the malformed third line: " + failureUrls, 1, failureUrls.size());
            assertTrue("failure id must point at real line 3, not line 2 - the blank line must not consume " + "a line-number slot: "
                    + failureUrls, failureUrls.get(0).endsWith("@3"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that a malformed line which does not start with '{' - a log line that found its way
     * into the file, say - is recorded as a failure on its own, and that the lines after it are
     * still indexed. JSON Lines is line-delimited, so one bad line has to cost exactly one line.
     *
     * <p>
     * Read as a token stream instead, {@code MappingIterator#hasNext()} wraps the
     * {@code JsonParseException} in a plain {@code RuntimeException} and throws it out of
     * storeData, taking down the whole data config. That storeData throws nothing is pinned here
     * too.
     * </p>
     */
    @Test
    public void test_storeData_recoversFromBareWordMalformedLine() throws Exception {
        final Path file = Files.createTempFile("bareword", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n" + "not json\n" + "{\"url\":\"http://example.com/3\"}\n"
                    + "{\"url\":\"http://example.com/4\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            // Must not throw: a malformed line is a record-level failure, not a crawl-level one.
            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("every valid line is stored: " + callback.getDataMapList(), 3, callback.getDataMapList().size());
            assertEquals("only the malformed line is recorded as a failure: " + failureUrls, 1, failureUrls.size());
            assertTrue("the failure points at line 2: " + failureUrls, failureUrls.get(0).endsWith("@2"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that a malformed line which does start with '{' is likewise recorded as a failure on
     * its own, with the lines after it still indexed. Read as a token stream, a failure on this
     * line ends the whole source and the third line disappears with neither a failure record nor
     * a log line.
     */
    @Test
    public void test_storeData_recoversFromBracePrefixedMalformedLine() throws Exception {
        final Path file = Files.createTempFile("braceprefixed", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n" + "{not valid json\n" + "{\"url\":\"http://example.com/3\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("the line after the malformed one is still read: " + callback.getDataMapList(), 2,
                    callback.getDataMapList().size());
            assertEquals("only the malformed line is recorded as a failure: " + failureUrls, 1, failureUrls.size());
            assertTrue("the failure points at line 2: " + failureUrls, failureUrls.get(0).endsWith("@2"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that an unknown format value becomes a failure record rather than an exception escaping
     * storeData. An escaping exception abandons the whole data config and leaves no failure
     * record behind.
     */
    @Test
    public void test_storeData_invalidFormatIsReportedNotThrown() throws Exception {
        final Path file = Files.createTempFile("badformat", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("format", "jsonlines");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            // Must not throw: an unknown format is a configuration error to report, not a crash.
            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("nothing is indexed with an unusable configuration", 0, callback.getDataMapList().size());
            assertEquals("the configuration error is recorded exactly once: " + failureUrls, 1, failureUrls.size());
            assertTrue("the failure names the offending parameter: " + failureUrls, failureUrls.get(0).contains("format"));
            assertTrue("the failure is reported as a DataStoreException: " + failureUrls,
                    failureUrls.get(0).startsWith("org.codelibs.fess.exception.DataStoreException @"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that an invalid regular expression in include_pattern or exclude_pattern becomes a
     * failure record named after that parameter, rather than a PatternSyntaxException escaping
     * storeData. An escaping exception abandons the whole data config and files the failure
     * against configId:name, so nothing in the record says which parameter was wrong.
     */
    @Test
    public void test_storeData_invalidPatternIsReportedNotThrown() throws Exception {
        assertPatternParameterIsReported("include_pattern");
        assertPatternParameterIsReported("exclude_pattern");
    }

    /**
     * Runs storeData with one parameter carrying an invalid regular expression and checks that it
     * produces a single failure record rather than an exception.
     *
     * @param patternParam the parameter to test
     * @throws Exception if the temporary file cannot be handled
     */
    private void assertPatternParameterIsReported(final String patternParam) throws Exception {
        final Path file = Files.createTempFile("badpattern", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n");
            failureUrls.clear();

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put(patternParam, "[unclosed");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            // Must not throw: a bad pattern is a configuration error to report, not a crash.
            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals(patternParam + ": nothing is indexed with an unusable configuration", 0, callback.getDataMapList().size());
            assertEquals(patternParam + ": the configuration error is recorded exactly once: " + failureUrls, 1, failureUrls.size());
            assertTrue(patternParam + ": the failure names the offending parameter: " + failureUrls,
                    failureUrls.get(0).endsWith("JsonDataStore:" + patternParam));
            assertTrue(patternParam + ": the failure is reported as a DataStoreException: " + failureUrls,
                    failureUrls.get(0).startsWith("org.codelibs.fess.exception.DataStoreException @"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that the not-yet-implemented urls parameter becomes a failure record named after that
     * parameter rather than an exception escaping storeData. An escaping exception abandons the
     * whole data config and files the failure against configId:name, so nothing in the record
     * says which parameter caused it.
     */
    @Test
    public void test_storeData_urlsIsReportedNotThrown() throws Exception {
        final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
        final DataStoreParams params = new DataStoreParams();
        params.put("urls", "http://example.com/a.json");
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");

        // Must not throw: an unsupported parameter is a configuration error to report.
        dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

        assertEquals("nothing is indexed", 0, callback.getDataMapList().size());
        assertEquals("the configuration error is recorded exactly once: " + failureUrls, 1, failureUrls.size());
        assertTrue("the failure names the offending parameter: " + failureUrls, failureUrls.get(0).endsWith("JsonDataStore:urls"));
        assertTrue("the failure is reported as a DataStoreException: " + failureUrls,
                failureUrls.get(0).startsWith("org.codelibs.fess.exception.DataStoreException @"));
    }

    /**
     * Test that the token-stream path gives up on a document truncated where it can never
     * resynchronize, instead of reading it forever, and says so in a warning.
     *
     * <p>
     * format=json forces the token stream. The file ends with its second line unclosed after one
     * good record, so Jackson keeps looking for the next record, keeps failing, and never reaches
     * the end of the stream. An implementation that gave up on the first failure would lose the
     * records after it in silence; one with no bound at all would never finish the crawl.
     * </p>
     */
    @Test
    public void test_storeData_truncatedTokenStreamStopsInsteadOfSpinning() throws Exception {
        final Path file = Files.createTempFile("truncated", ".json");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n" + "{\"url\":\n" + "{\"url\":\"http://example.com/3\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("format", "json");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            final List<String> logMessages =
                    captureDataStoreLogs(() -> dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>()));

            assertEquals("the record before the truncation is stored", 1, callback.getDataMapList().size());
            assertEquals("one unparseable region is one failure record, however many times the parser trips over " + "it: " + failureUrls,
                    1, failureUrls.size());
            assertTrue("giving up must be said out loud, naming the source and carrying the real total: " + logMessages, logMessages
                    .stream()
                    .anyMatch(m -> m.startsWith("Gave up on ") && m.contains(file.toString())
                            && m.contains("after " + JsonDataStore.MAX_CONSECUTIVE_TOKEN_FAILURES + " consecutive parse failures")));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that none of the lines after a broken first line are lost, in three shapes of broken
     * first line: a bare word, one starting with '{', and one cut off mid-record. AUTO is the
     * default, and a download that stopped halfway looks exactly like the third. A look-ahead
     * that judged the document from its first line alone would take these for a pretty-printed
     * single object, read them as a token stream, and lose the records after them in silence.
     */
    @Test
    public void test_storeData_recoversFromMalformedFirstLine() throws Exception {
        final String tail = "{\"url\":\"http://example.com/2\"}\n{\"url\":\"http://example.com/3\"}\n";
        assertFirstLineShape("bare word", "not json\n" + tail);
        assertFirstLineShape("brace prefixed", "{not valid json\n" + tail);
        assertFirstLineShape("truncated", "{\"url\":\n" + tail);
    }

    /**
     * Runs storeData over a file whose first line is broken and checks that the remaining two
     * records are indexed and exactly one failure is recorded.
     *
     * @param shape the name of the shape, used in assertion messages
     * @param content the file content: a malformed first line followed by two good records
     * @throws Exception if the temporary file cannot be handled
     */
    private void assertFirstLineShape(final String shape, final String content) throws Exception {
        final Path file = Files.createTempFile("firstline", ".jsonl");

        try {
            Files.writeString(file, content);
            failureUrls.clear();

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals(shape + ": the two records after the bad first line are stored: " + callback.getDataMapList(), 2,
                    callback.getDataMapList().size());
            assertEquals(shape + ": only the first line is recorded as a failure: " + failureUrls, 1, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that a single object whose second line merely starts with a complete object is not
     * mistaken for JSON Lines. {@code {"a":\n{"b":1}}} is one valid JSON document, and its second
     * line {@code {"b":1}}} is a complete object followed by nothing but a stray {@code }}.
     * Unless the second line is judged strictly, the document goes down the line path and records
     * are lost.
     */
    @Test
    public void test_storeData_valueOnNextLineIsNotJsonLines() throws Exception {
        final Path file = Files.createTempFile("nextline", ".json");

        try {
            Files.writeString(file, "{\"url\":\n{\"href\":\"http://example.com/1\"}}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("the whole object is one record: " + callback.getDataMapList(), 1, callback.getDataMapList().size());
            assertEquals("no failures: " + failureUrls, 0, failureUrls.size());
            final Object url = callback.getDataMapList().get(0).get("url");
            assertTrue("the nested value survives, so the object was not read line by line: " + url, url instanceof Map);
            assertEquals("http://example.com/1", ((Map<?, ?>) url).get("href"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * The same shape as above with a sibling field added after the nested object. Its second line
     * {@code {"x":1},} is a complete object followed by a comma, not a record. Mistaken for one,
     * the whole object is chopped into three lines.
     */
    @Test
    public void test_storeData_valueOnNextLineWithSiblingFieldIsNotJsonLines() throws Exception {
        final Path file = Files.createTempFile("nextlinesibling", ".json");

        try {
            Files.writeString(file, "{\"data\":\n{\"x\":1},\n\"url\":\"http://example.com/1\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("the whole object is one record: " + callback.getDataMapList(), 1, callback.getDataMapList().size());
            assertEquals("no failures: " + failureUrls, 0, failureUrls.size());
            assertEquals("the field after the nested value is read too", "http://example.com/1",
                    callback.getDataMapList().get(0).get("url"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Pins, explicitly, that a line holding two objects still indexes only the first of them, as
     * it did before this phase. The second line is judged strictly while the first is left
     * deliberately lenient, and that asymmetry is not to be tightened in silence later.
     */
    @Test
    public void test_storeData_twoObjectsOnFirstLineStillYieldsOnlyTheFirst() throws Exception {
        final Path file = Files.createTempFile("twoonaline", ".jsonl");

        try {
            Files.writeString(file,
                    "{\"url\":\"http://example.com/1\"}{\"url\":\"http://example.com/2\"}\n" + "{\"url\":\"http://example.com/3\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("the second object on line 1 is still dropped: " + callback.getDataMapList(), 2, callback.getDataMapList().size());
            assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));
            assertEquals("http://example.com/3", callback.getDataMapList().get(1).get("url"));
            assertEquals("still silent about it: " + failureUrls, 0, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that nothing from the third line on is lost when the second line of a JSON Lines file
     * is broken as well as the first. A two-line banner, or two lines of transfer progress, above
     * an export is an ordinary shape, and the merge base (1aaa07a) lost exactly those two lines
     * and indexed the rest. A look-ahead that stopped after two lines would fall back to the
     * token stream once both had failed, and lose the whole source.
     */
    @Test
    public void test_storeData_twoBadLeadingLinesDoNotCostTheRecordsBelow() throws Exception {
        final Path file = Files.createTempFile("twobanner", ".jsonl");

        try {
            Files.writeString(file, "garbage one\ngarbage two\n{\"url\":\"http://example.com/3\"}\n{\"url\":\"http://example.com/4\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("both records below the two bad lines are stored: " + callback.getDataMapList(), 2,
                    callback.getDataMapList().size());
            assertEquals("http://example.com/3", callback.getDataMapList().get(0).get("url"));
            assertEquals("http://example.com/4", callback.getDataMapList().get(1).get("url"));
            assertEquals("each bad line costs exactly itself: " + failureUrls, 2, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that every record of a file whose lines end in a bare {@code '\r'} is indexed. Unless
     * {@code '\r'} counts as a line terminator the whole file is one line, and the lenient parse
     * returns only its first record while the rest disappear with neither a failure record nor a
     * log line - the one silently-losing path this branch had.
     */
    @Test
    public void test_storeData_crOnlyLineEndings() throws Exception {
        final Path file = Files.createTempFile("cronly", ".jsonl");

        try {
            Files.write(file, "{\"url\":\"http://example.com/1\"}\r{\"url\":\"http://example.com/2\"}\r{\"url\":\"http://example.com/3\"}\r"
                    .getBytes(StandardCharsets.UTF_8));

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("all three CR-terminated records are stored: " + callback.getDataMapList(), 3, callback.getDataMapList().size());
            assertEquals("http://example.com/3", callback.getDataMapList().get(2).get("url"));
            assertEquals("no failures: " + failureUrls, 0, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that a CRLF file reads without a blank line between records, that is, that
     * {@code "\r\n"} is not counted as two terminators. A blank line between them would put the
     * line numbers of failures out of step with the real file.
     */
    @Test
    public void test_storeData_crlfLineEndings() throws Exception {
        final Path file = Files.createTempFile("crlf", ".jsonl");

        try {
            Files.write(file, "{\"url\":\"http://example.com/1\"}\r\n{not valid json\r\n{\"url\":\"http://example.com/3\"}\r\n"
                    .getBytes(StandardCharsets.UTF_8));

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("both valid records are stored: " + callback.getDataMapList(), 2, callback.getDataMapList().size());
            assertEquals("only the malformed line fails: " + failureUrls, 1, failureUrls.size());
            assertTrue("the failure points at real line 2, so no phantom blank lines were counted: " + failureUrls,
                    failureUrls.get(0).endsWith("@2"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that root_path together with format=jsonl leaves a warning rather than dropping the
     * format in silence. A document navigated by a JSON Pointer has a structure and can only be
     * read as a token stream, so the resolution itself is right - but it should not happen
     * quietly.
     */
    @Test
    public void test_storeData_rootPathWithFormatJsonlIsWarned() throws Exception {
        final Path file = Files.createTempFile("rootpathjsonl", ".json");

        try {
            Files.writeString(file, "{\"data\":{\"items\":[{\"url\":\"http://example.com/1\"}]}}");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("root_path", "/data/items");
            params.put("format", "jsonl");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            final List<String> logMessages =
                    captureDataStoreLogs(() -> dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>()));

            assertEquals("the nested record is still read", 1, callback.getDataMapList().size());
            assertTrue("the ignored parameter must be named out loud: " + logMessages,
                    logMessages.stream().anyMatch(m -> m.contains("format is ignored because root_path is set")));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that one malformed element inside an array is reported as a single failure record and
     * that both good elements around it are still indexed. A token stream steps over an
     * unparseable stretch one character at a time and fails again for each, so recording every
     * one of those would pile identical failures against the same URL. The count alone would stay
     * green even if the element after the malformed one vanished entirely, and that is the
     * heavier guarantee of the two.
     */
    @Test
    public void test_storeData_oneBadArrayElementIsOneFailure() throws Exception {
        final Path file = Files.createTempFile("badelement", ".json");

        try {
            Files.writeString(file, "[\n" + "  {\"url\":\"http://example.com/1\"},\n" + "  {not an element},\n"
                    + "  {\"url\":\"http://example.com/3\"}\n" + "]\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("one bad element is one failure record, not one per parser hiccup: " + failureUrls, 1, failureUrls.size());
            assertEquals("the elements on either side of the bad one are both stored: " + callback.getDataMapList(), 2,
                    callback.getDataMapList().size());
            assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));
            assertEquals("the token stream recovers past the bad element rather than dropping the rest of the array",
                    "http://example.com/3", callback.getDataMapList().get(1).get("url"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that every record of a JSON array file is indexed.
     */
    @Test
    public void test_storeData_jsonArrayFile() throws Exception {
        final Path file = Files.createTempFile("array", ".json");
        Files.writeString(file, "[\n  {\"url\":\"http://example.com/1\"},\n  {\"url\":\"http://example.com/2\"}\n]\n");

        try {
            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("both array elements are stored", 2, callback.getDataMapList().size());
            assertEquals("no failures: " + failureUrls, 0, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that the good lines after a broken first line are still indexed when the JSON Lines
     * records are indented. The merge base (1aaa07a) read every document line by line
     * unconditionally and so indexed two records from this shape; a look-ahead that took only
     * lines starting in column zero for records would drop indented JSON Lines onto the token
     * stream and lose the source. Both space and tab indentation are pinned.
     */
    @Test
    public void test_storeData_indentedJsonLinesWithMalformedFirstLine() throws Exception {
        assertIndentedJsonLines("space-indented", "  ");
        assertIndentedJsonLines("tab-indented", "\t");
    }

    /**
     * Runs storeData over indented JSON Lines and checks the one failure on the first line and the
     * two records indexed after it.
     *
     * @param shape the name of the shape, used in assertion messages
     * @param indent the whitespace prefixed to every line
     * @throws Exception if the temporary file cannot be handled
     */
    private void assertIndentedJsonLines(final String shape, final String indent) throws Exception {
        final Path file = Files.createTempFile("indented", ".jsonl");

        try {
            Files.writeString(file, indent + "garbage\n" + indent + "{\"url\":\"http://example.com/1\"}\n" + indent
                    + "{\"url\":\"http://example.com/2\"}\n");
            failureUrls.clear();

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals(shape + ": both indented records are stored: " + callback.getDataMapList(), 2, callback.getDataMapList().size());
            assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));
            assertEquals("http://example.com/2", callback.getDataMapList().get(1).get("url"));
            assertEquals(shape + ": only the malformed first line fails: " + failureUrls, 1, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that a pretty-printed wrapper object read with a root_path yields the elements of its
     * nested array. This is the only practical configuration for that shape, and no change to
     * AUTO detection may affect it: root_path skips the look-ahead entirely.
     */
    @Test
    public void test_storeData_rootPathOnPrettyPrintedWrapper() throws Exception {
        final Path file = Files.createTempFile("prettywrapper", ".json");

        try {
            Files.writeString(file, PRETTY_WRAPPER);

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("root_path", "/items");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("both nested records are stored: " + callback.getDataMapList(), 2, callback.getDataMapList().size());
            assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));
            assertEquals("http://example.com/2", callback.getDataMapList().get(1).get("url"));
            assertEquals("no failures: " + failureUrls, 0, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Pins, explicitly, what reading that same wrapper without a root_path does.
     *
     * <p>
     * This is the price of AUTO detection. The pretty-printed wrapper is chopped up line by line:
     * its two array elements are indexed and the remaining five lines are recorded as failures.
     * The merge base (1aaa07a) read every document line by line unconditionally, so it produced
     * exactly the same result. Reading this shape as one record without a root_path looks better
     * until you see what it yields - a single document whose url is null. Writing the price down
     * keeps the decision from being reversed by accident.
     * </p>
     */
    @Test
    public void test_storeData_prettyPrintedWrapperWithoutRootPathIsReadLineByLine() throws Exception {
        final Path file = Files.createTempFile("prettywrappernoroot", ".json");

        try {
            Files.writeString(file, PRETTY_WRAPPER);

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("the two array element lines are stored as records: " + callback.getDataMapList(), 2,
                    callback.getDataMapList().size());
            assertEquals("http://example.com/1", callback.getDataMapList().get(0).get("url"));
            assertEquals("http://example.com/2", callback.getDataMapList().get(1).get("url"));
            assertEquals("the other five lines are recorded as failures, exactly as 1aaa07a did: " + failureUrls, 5, failureUrls.size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that root_path pulls the records out of a nested array.
     */
    @Test
    public void test_storeData_rootPath() throws Exception {
        final Path file = Files.createTempFile("nested", ".json");
        Files.writeString(file, "{\"data\":{\"items\":[{\"url\":\"http://example.com/1\"},{\"url\":\"http://example.com/2\"}]}}");

        try {
            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("root_path", "/data/items");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("both nested elements are stored", 2, callback.getDataMapList().size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that sensitive parameters cannot be read from a script.
     */
    @Test
    public void test_storeData_doesNotExposeSecretsToScript() throws Exception {
        final Path file = Files.createTempFile("secret", ".jsonl");
        Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n");

        try {
            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("crawler.web.auth.a.password", "s3cr3t");
            params.put("access_token", "t0ken");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");
            scriptMap.put("leaked_password", "crawler.web.auth.a.password");
            scriptMap.put("leaked_token", "access_token");
            scriptMap.put("visible_files", "files");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals(1, callback.getDataMapList().size());
            final Map<String, Object> dataMap = callback.getDataMapList().get(0);
            assertNull("the password must not reach the document", dataMap.get("leaked_password"));
            assertNull("the token must not reach the document", dataMap.get("leaked_token"));
            assertNotNull("non-sensitive parameters stay visible", dataMap.get("visible_files"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that reaching a sensitive parameter through the script engine rather than by an exact
     * match - by concatenating it - still does not recover the secret. JavaScriptEngine
     * stringifies a null operand, so the result is "[null]" rather than an empty string.
     */
    @Test
    public void test_storeData_scriptConcatenationOfFilteredParameterYieldsNullString() throws Exception {
        final Path file = Files.createTempFile("secret", ".jsonl");
        Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n");

        try {
            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("access_token", "t0ken");
            // Only this test needs an engine at all, and an unset script type resolves to Groovy
            // (AbstractDataStore#getScriptType returns Constants.LEGACY_SCRIPT), which ships as a
            // separate plugin and is not on this test's classpath. Name the engine setUp registers.
            params.put("script_type", Constants.DEFAULT_SCRIPT);
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");
            // Not an exact-match template, so convertValue falls through to the real
            // JavaScriptEngine registered in setUp() instead of the containsKey fast path.
            scriptMap.put("wrapped_token", "'[' + access_token + ']'");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals(1, callback.getDataMapList().size());
            final Object wrapped = callback.getDataMapList().get(0).get("wrapped_token");
            assertEquals("[null]", wrapped);
            assertFalse("the secret must not leak through concatenation either", wrapped != null && wrapped.toString().contains("t0ken"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that a record field takes priority even when it has the same name as a filtered
     * parameter.
     */
    @Test
    public void test_storeData_recordFieldOverridesFilteredParameter() throws Exception {
        final Path file = Files.createTempFile("secret", ".jsonl");
        Files.writeString(file, "{\"url\":\"http://example.com/1\",\"access_token\":\"record-value\"}\n");

        try {
            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            params.put("access_token", "t0ken");
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");
            scriptMap.put("token_field", "access_token");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals(1, callback.getDataMapList().size());
            assertEquals("record-value", callback.getDataMapList().get(0).get("token_field"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Test that filtering falls back to the default pattern and carries on when
     * {@link ComponentUtil#getFessConfig()} returns {@code null}.
     */
    @Test
    public void test_resolveEncryptPropertyPattern_nullConfigFallsBackToDefault() throws Exception {
        final JsonDataStore store = new JsonDataStore() {
            @Override
            protected FessConfig fetchFessConfig() {
                return null;
            }
        };

        final Pattern pattern = store.resolveEncryptPropertyPattern();

        assertTrue("a sensitive key must still be recognized", pattern.matcher("crawler.web.auth.a.password").matches());
        assertFalse("a non-sensitive key must not be blanked", pattern.matcher("files").matches());
    }

    /**
     * Test that filtering falls back to the default pattern and carries on when the configured
     * pattern is blank.
     */
    @Test
    public void test_resolveEncryptPropertyPattern_blankPatternFallsBackToDefault() throws Exception {
        final JsonDataStore store = new JsonDataStore() {
            @Override
            protected FessConfig fetchFessConfig() {
                return fessConfigReturningPattern("   ");
            }
        };

        final Pattern pattern = store.resolveEncryptPropertyPattern();

        assertTrue("a sensitive key must still be recognized", pattern.matcher("crawler.web.auth.a.password").matches());
        assertFalse("a non-sensitive key must not be blanked", pattern.matcher("files").matches());
    }

    /**
     * Test that filtering falls back to the default pattern and carries on when the configured
     * pattern is not a valid regular expression, that is, when Pattern.compile throws a
     * PatternSyntaxException.
     */
    @Test
    public void test_resolveEncryptPropertyPattern_malformedPatternFallsBackToDefault() throws Exception {
        final JsonDataStore store = new JsonDataStore() {
            @Override
            protected FessConfig fetchFessConfig() {
                return fessConfigReturningPattern("[unclosed");
            }
        };

        final Pattern pattern = store.resolveEncryptPropertyPattern();

        assertTrue("a sensitive key must still be recognized", pattern.matcher("crawler.web.auth.a.password").matches());
        assertFalse("a non-sensitive key must not be blanked", pattern.matcher("files").matches());
    }

    /**
     * Test that filtering falls back to the default pattern and carries on when the FessConfig
     * lookup itself throws.
     */
    @Test
    public void test_resolveEncryptPropertyPattern_configAccessorThrowsFallsBackToDefault() throws Exception {
        final JsonDataStore store = new JsonDataStore() {
            @Override
            protected FessConfig fetchFessConfig() {
                throw new IllegalStateException("simulated: FessConfig component not available");
            }
        };

        final Pattern pattern = store.resolveEncryptPropertyPattern();

        assertTrue("a sensitive key must still be recognized", pattern.matcher("crawler.web.auth.a.password").matches());
        assertFalse("a non-sensitive key must not be blanked", pattern.matcher("files").matches());
    }

    /**
     * Builds a minimal {@link FessConfig} stub for tests that call nothing but
     * {@link FessConfig#getAppEncryptPropertyPattern()}. A dynamic proxy answering that one call
     * stands in for a full implementation of the (several hundred method) interface.
     *
     * @param pattern the value {@link FessConfig#getAppEncryptPropertyPattern()} returns
     * @return a {@link FessConfig} that answers with that value and nothing else
     */
    private static FessConfig fessConfigReturningPattern(final String pattern) {
        return (FessConfig) java.lang.reflect.Proxy.newProxyInstance(FessConfig.class.getClassLoader(), new Class<?>[] { FessConfig.class },
                (proxy, method, args) -> {
                    if ("getAppEncryptPropertyPattern".equals(method.getName())) {
                        return pattern;
                    }
                    throw new UnsupportedOperationException(
                            "This stub only implements getAppEncryptPropertyPattern(); called: " + method.getName());
                });
    }

    /**
     * Runs {@code action} with an appender attached to {@link JsonDataStore}'s logger and
     * returns the messages it logged.
     *
     * <p>
     * The logger's level is forced to WARN for the duration: without a log4j2 configuration on
     * the test classpath the default root level is ERROR, which would drop the very warning
     * under test.
     * </p>
     *
     * @param action the work to run
     * @return the formatted messages logged while it ran
     */
    private List<String> captureDataStoreLogs(final Runnable action) {
        final List<String> messages = Collections.synchronizedList(new ArrayList<>());
        final AbstractAppender appender = new AbstractAppender("jsonDataStoreCapture", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(final LogEvent event) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        };
        appender.start();
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(JsonDataStore.class);
        final Level originalLevel = coreLogger.getLevel();
        coreLogger.addAppender(appender);
        coreLogger.setLevel(Level.WARN);
        try {
            action.run();
        } finally {
            coreLogger.setLevel(originalLevel);
            coreLogger.removeAppender(appender);
            appender.stop();
        }
        return messages;
    }

    /**
     * A line holding only null is not a record. It was already recorded as a failure, but as a
     * NullPointerException raised while merging the record into the script scope, which names
     * neither the line nor what was wrong with it.
     */
    @Test
    public void test_storeData_nullLineIsRecordedAsFailure() throws Exception {
        final Path file = Files.createTempFile("nullline", ".jsonl");

        try {
            Files.writeString(file, "{\"url\":\"http://example.com/1\"}\n" + "null\n" + "{\"url\":\"http://example.com/2\"}\n");

            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final DataStoreParams params = new DataStoreParams();
            params.put("files", file.toString());
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");

            dataStore.storeData(new DataConfig(), callback, params, scriptMap, new HashMap<>());

            assertEquals("the lines either side of the null are still stored: " + failureUrls, 2, callback.getDataMapList().size());
            assertEquals("exactly one failure is recorded: " + failureUrls, 1, failureUrls.size());
            assertTrue("the failure is filed against line 2: " + failureUrls, failureUrls.get(0).endsWith("@2"));
            assertTrue("the line is reported as a data store error, not a NullPointerException: " + failureUrls,
                    failureUrls.get(0).startsWith(DataStoreException.class.getName()));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Helper method to invoke private methods using reflection.
     */

    @SuppressWarnings("unchecked")
    private <T> T invokeMethod(Object obj, String methodName, Object... args) throws Exception {
        Class<?>[] paramTypes = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) {
            paramTypes[i] = args[i].getClass();
        }

        java.lang.reflect.Method method = null;

        // Try to find method with exact parameter types
        try {
            method = obj.getClass().getDeclaredMethod(methodName, paramTypes);
        } catch (NoSuchMethodException e) {
            // If not found, try to find method by name and parameter count
            for (java.lang.reflect.Method m : obj.getClass().getDeclaredMethods()) {
                if (m.getName().equals(methodName) && m.getParameterCount() == args.length) {
                    method = m;
                    break;
                }
            }
        }

        if (method == null) {
            throw new NoSuchMethodException(methodName);
        }

        method.setAccessible(true);
        return (T) method.invoke(obj, args);
    }

    /**
     * Test implementation of IndexUpdateCallback for testing purposes.
     */
    static class TestIndexUpdateCallback implements IndexUpdateCallback {
        private final List<Map<String, Object>> dataMapList = new ArrayList<>();

        @Override
        public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
            dataMapList.add(new HashMap<>(dataMap));
        }

        @Override
        public long getDocumentSize() {
            return dataMapList.size();
        }

        @Override
        public long getExecuteTime() {
            return 0;
        }

        @Override
        public void commit() {
            // nothing to do
        }

        List<Map<String, Object>> getDataMapList() {
            return dataMapList;
        }
    }

}
