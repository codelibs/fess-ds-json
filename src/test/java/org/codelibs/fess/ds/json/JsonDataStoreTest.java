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
import org.codelibs.fess.opensearch.config.exentity.CrawlingConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.opensearch.config.exentity.FailureUrl;
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
    public JsonDataStore dataStore;

    /** FailureUrlService に記録された失敗を "<errorName> @ <url>" 形式で保持する。 */
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

        // storeData は CrawlerStatsHelper を使い、CrawlerStatsHelper は SystemHelper を使う。
        // 初期化済みインスタンスを登録してパイプライン全体をテスト内で通せるようにする。
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");

        // 失敗記録の本物は OpenSearch を要求するため、記録内容だけを控える no-op に差し替える。
        // ComponentUtil.getComponent(Class) が解決できるよう正準名で登録する。
        ComponentUtil.register(new FailureUrlService() {
            @Override
            public FailureUrl store(final CrawlingConfig crawlingConfig, final String errorName, final String url, final Throwable e) {
                failureUrls.add(errorName + " @ " + url);
                return null;
            }
        }, FailureUrlService.class.getCanonicalName());
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
     * setFileSuffixes による DI 経由の設定が resolver に伝わることを検証する。
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
     * storeData が JSON/JSONL の全レコードを callback に渡すことを検証する。
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
     * 拡張子が対象外のファイルが処理されないことを検証する。
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
     * 空行・空白行が失敗として記録されずスキップされることを検証する。
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
     * UTF-8 BOM 付きファイルの先頭レコードが読めることを検証する。
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
     * stop() 後は alive が false になり、レコードが処理されないことを検証する。
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
     * alive をファイルループの外側だけでなく processSource 内のレコードループでも
     * 見ていることを検証する。1件目を store() した直後に stop() を呼び、2件目が
     * 処理されないことを、件数だけでなく内容（1件目の url）でも確認する。件数だけの
     * 比較だと、内側の alive チェックが失われて2件目が残ってしまっても検出できない。
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
     * 読み取れないファイルが失敗として記録されることを検証する。
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
     * DataStoreCrawlingException(url, message, cause, true) が store() から投げられたとき、
     * processSource のレコードループとソースループの両方が break することを検証する。
     * 1ファイル目に2件、2ファイル目に1件のレコードを置き、1件目の store() で abort する
     * コールバックを使う。2ファイル目は決して開かれないため、試みられるレコードは1件だけ。
     * failureUrls の errorName が例外そのものではなく cause (IllegalStateException) から、
     * url が StatsKeyObject の id ではなく DataStoreCrawlingException#getUrl() から取られる
     * ことも同時に固定する。
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
     * 空行を挟んでも行番号が実ファイルの行番号を追い続けることを検証する。1行目は
     * 正常なレコード、2行目は空行、3行目は不正な JSON。記録される失敗の id が "@2" では
     * なく "@3" で終わることを確認する。JsonRecordReader#getCurrentLineNumber() が返す
     * 行番号をレコードカウンタの代わりに使う必要があり、そうでなければ失敗はエディタ上の
     * 実際の行を指さなくなる。
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
     * 先頭が '{' ではない不正な行（紛れ込んだログ行など）があっても、その行だけが失敗として
     * 記録され、後続の行が登録されることを検証する。JSONL は行区切りなので、1行の失敗が
     * 1行分のコストで済まなければならない。
     *
     * <p>
     * トークンストリームで読むと {@code MappingIterator#hasNext()} が {@code JsonParseException} を
     * 素の {@code RuntimeException} で包んで投げ、それが storeData の外まで抜けてデータ設定
     * 全体を落とす。storeData が例外を投げないことも同時に固定する。
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
     * '{' で始まる不正な行があっても、その行だけが失敗として記録され、後続の行が登録される
     * ことを検証する。トークンストリームで読むとこの行の失敗はソース全体の打ち切りになり、
     * 3行目が失敗記録もログもないまま消える。
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
     * format に未知の値を書いた設定ミスが、storeData から抜ける例外ではなく失敗記録になる
     * ことを検証する。抜けるとデータ設定全体が中断され、失敗記録も残らない。
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
     * 途中で切れて二度と同期し直せないドキュメントを、トークンストリーム経路が
     * 無限に読み続けずに打ち切り、その旨を警告に残すことを検証する。
     *
     * <p>
     * format=json でトークンストリームを強制する。1件目のあと 2行目が閉じられないまま
     * ファイルが終わるので、Jackson は次のレコードを探しては失敗し続け、ストリーム終端にも
     * 到達しない。最初の失敗で打ち切る実装だと後続が黙って消え、上限を持たない実装だと
     * クロールが終わらない。
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
            assertTrue("the token stream must keep trying past the first failure and then give up, not stop at one "
                    + "and not run forever: " + failureUrls.size(), failureUrls.size() > 1 && failureUrls.size() <= 100);
            assertTrue("giving up must be said out loud, naming the source: " + logMessages,
                    logMessages.stream().anyMatch(m -> m.startsWith("Gave up on ") && m.contains(file.toString())));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * JSON 配列ファイルが全レコード登録されることを検証する。
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
     * root_path でネストした配列を取り出せることを検証する。
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
