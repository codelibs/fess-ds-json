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
import org.codelibs.fess.script.groovy.GroovyEngine;
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

        // 実際の GroovyEngine を登録する。scriptMap のテンプレートが resultMap の完全一致キーで
        // ないとき（例: 連結式）、AbstractDataStore#convertValue がここを経由する。DI コンテナ経由
        // ではなく直接 new するため @PostConstruct init() は呼ばれないが、evaluate() が必要とする
        // スクリプトキャッシュはコンストラクタの buildScriptCache() だけで揃う。
        final ScriptEngineFactory scriptEngineFactory = new ScriptEngineFactory();
        scriptEngineFactory.add(Constants.DEFAULT_SCRIPT, new GroovyEngine());
        ComponentUtil.register(scriptEngineFactory, "scriptEngineFactory");
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
     * 旧名 fileEncoding が引き続き効くことを検証する（ParamMap の自動変換による）。
     * あわせて、内部パラメータ名が file_encoding に改名されたことを確認する
     * （ParamMap はどちらの綴りで問い合わせても同じ値を返すため、改名自体は
     * この reflection によるチェックでしか黒箱的に検証できない）。
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
     * 新名 file_encoding が素直に効くことを検証する（旧名だけを試すテストの裏返し）。
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
     * include_pattern / exclude_pattern に不正な正規表現を書いた設定ミスが、storeData から
     * 抜ける PatternSyntaxException ではなく、当該パラメータ名を持つ失敗記録になることを
     * 検証する。抜けるとデータ設定全体が中断され、失敗は configId:name に帰属して
     * どのパラメータが悪いのか記録に残らない。
     */
    @Test
    public void test_storeData_invalidPatternIsReportedNotThrown() throws Exception {
        assertPatternParameterIsReported("include_pattern");
        assertPatternParameterIsReported("exclude_pattern");
    }

    /**
     * 不正な正規表現を持つパラメータ1つを storeData に通し、例外ではなく失敗記録1件に
     * なることを検証する。
     *
     * @param patternParam 検証するパラメータ名
     * @throws Exception 一時ファイルの操作に失敗した場合
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
     * まだ実装されていない urls パラメータが、storeData から抜ける例外ではなく、
     * 当該パラメータ名を持つ失敗記録になることを検証する。抜けるとデータ設定全体が
     * 中断され、失敗は configId:name に帰属して、どのパラメータが原因か記録に残らない。
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
     * 1行目が壊れている3つの形でも2行目以降が失われないことを検証する。bare word、
     * '{' 始まり、そして途中で切れた1行目。AUTO は既定であり、途中で切れたダウンロードは
     * まさにこの形になる。1行目だけを見て判定すると、これらは「整形済み単一オブジェクト」と
     * 誤判定されてトークンストリームに載り、後続レコードが黙って消える。
     */
    @Test
    public void test_storeData_recoversFromMalformedFirstLine() throws Exception {
        final String tail = "{\"url\":\"http://example.com/2\"}\n{\"url\":\"http://example.com/3\"}\n";
        assertFirstLineShape("bare word", "not json\n" + tail);
        assertFirstLineShape("brace prefixed", "{not valid json\n" + tail);
        assertFirstLineShape("truncated", "{\"url\":\n" + tail);
    }

    /**
     * 1行目が壊れたファイルを storeData に通し、残り2レコードが登録され失敗が1件だけ
     * 記録されることを検証する。
     *
     * @param shape 失敗時のメッセージに使う形の名前
     * @param content ファイル内容。1行目が不正、続く2行が正常なレコード
     * @throws Exception 一時ファイルの操作に失敗した場合
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
     * 2行目が「完全なオブジェクトで始まる」だけの単一オブジェクトが、JSON Lines と
     * 誤判定されないことを検証する。{@code {"a":\n{"b":1}}} は妥当な JSON 1件だが、
     * 2行目 {@code {"b":1}}} は完全なオブジェクトの後ろに余分な {@code }} が続くだけである。
     * 2行目の判定を厳密にしないと行経路に載り、レコードが失われる。
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
     * 上と同じ形に、入れ子の後ろへ兄弟フィールドを足したもの。2行目
     * {@code {"x":1},} は完全なオブジェクトのあとにカンマが続くだけで、レコードではない。
     * 誤判定されるとオブジェクト全体が3行に切り刻まれる。
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
     * 1行に2件のオブジェクトが並ぶ行から先頭の1件だけが登録される、という本フェーズ以前からの
     * 挙動が変わっていないことを明示的に固定する。2行目の判定を厳密にした一方で、1行目の判定は
     * 意図的に緩いままである。将来これを黙って引き締められないようにする。
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
     * 1行目だけでなく2行目も壊れている JSONL から、3行目以降が失われないことを検証する。
     * 2行のバナーや2行分の転送進捗が JSONL エクスポートの上に付くのはありふれた形であり、
     * マージ元 (1aaa07a) はそれらの2行だけを失って残りを登録していた。先読みが最初の2行で
     * 打ち切ると、両方失敗した時点でトークンストリームに落ち、ソース全体が消える。
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
     * 行末が {@code '\r'} だけのファイルでも全レコードが登録されることを検証する。
     * {@code '\r'} を行終端として扱わないとファイル全体が1行になり、寛容なパースが
     * 先頭の1件だけを返して残りが失敗記録もログもなく消える。本ブランチで唯一の
     * 「黙って失われる」経路だった。
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
     * CRLF のファイルが、{@code "\r\n"} を2つの終端と数えて空行を挟むことなく読めることを
     * 検証する。空行が挟まると失敗の行番号が実ファイルの行とずれる。
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
     * root_path と format=jsonl が同時に指定されたとき、format が黙って捨てられるのではなく
     * 警告として残ることを検証する。JSON Pointer で辿るドキュメントは構造を持つため
     * トークンストリームで読むしかなく、解決自体は正しいが、黙って行うべきではない。
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
     * 配列内の1件の不正な要素が、失敗記録1件で報告され、かつ前後の正常な要素が2件とも
     * 登録されることを検証する。トークンストリームは不正な区間を1文字ずつ踏み越えながら
     * 失敗を繰り返すため、素直に記録すると同じ URL に対する同一の失敗記録が積み上がる。
     * 失敗件数だけを見ると、不正要素のあとの要素が丸ごと消えても緑のままになる。そちらが
     * 保証として重い。
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
     * 行頭に空白がある JSONL でも、壊れた1行目のあとの正常な行が登録されることを検証する。
     * マージ元 (1aaa07a) は無条件に行単位で読んでいたためこの形を 2件登録しており、
     * 先読みが「行頭が空白でない行だけを記録とみなす」と、インデントされた JSONL が
     * まるごとトークンストリームに落ちてソースが消える。空白インデントとタブインデントの
     * 両方を固定する。
     */
    @Test
    public void test_storeData_indentedJsonLinesWithMalformedFirstLine() throws Exception {
        assertIndentedJsonLines("space-indented", "  ");
        assertIndentedJsonLines("tab-indented", "\t");
    }

    /**
     * インデントされた JSONL を storeData に通し、1行目の失敗1件と後続2件の登録を確認する。
     *
     * @param shape 失敗時のメッセージに使う形の名前
     * @param indent 各行の先頭に付ける空白
     * @throws Exception 一時ファイルの操作に失敗した場合
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
     * 整形済みラッパーオブジェクトを root_path 付きで読んだとき、ネストした配列が正しく
     * 取り出せることを検証する。これがこの形の唯一の実用的な設定であり、AUTO 判定を
     * どう変えてもここは影響を受けてはならない (root_path は先読み自体を飛ばす)。
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
     * 同じラッパーを root_path なしで読んだときに何が起きるかを明示的に固定する。
     *
     * <p>
     * これは AUTO 判定の代償そのものである。整形済みラッパーは行単位に切り刻まれ、
     * 中の配列要素2件が登録され、残り5行が失敗として記録される。マージ元 (1aaa07a) は
     * 無条件に行単位で読んでいたので、まったく同じ結果になる。root_path なしでこの形を
     * 1レコードとして読む方が良く見えるが、それは url が null の文書1件にしかならない。
     * 代償を明示的に書き出しておくことで、この判断を暗黙に覆せないようにする。
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
     * 機微パラメータがスクリプトから読めないことを検証する。
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
     * 完全一致ではなくスクリプトエンジン経由（連結式）で機微パラメータに触れても、
     * 秘密の値自体は取り出せないことを検証する。GroovyEngine は null オペランドを
     * 文字列化するため、結果は空文字ではなく "[null]" になる。
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
            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url");
            // Not an exact-match template, so convertValue falls through to the real
            // GroovyEngine registered in setUp() instead of the containsKey fast path.
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
     * レコード側のフィールドが、フィルタ済みパラメータと同名であっても優先されることを検証する。
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
     * {@link ComponentUtil#getFessConfig()} が {@code null} を返す場合でも、
     * フィルタリングが既定パターンへフォールバックして継続することを検証する。
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
     * 設定されたパターンが空文字の場合でも、フィルタリングが既定パターンへ
     * フォールバックして継続することを検証する。
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
     * 設定されたパターンが不正な正規表現（Pattern.compile が PatternSyntaxException を投げる）の
     * 場合でも、フィルタリングが既定パターンへフォールバックして継続することを検証する。
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
     * FessConfig へのアクセス自体が例外を投げる場合でも、フィルタリングが既定パターンへ
     * フォールバックして継続することを検証する。
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
     * {@link FessConfig#getAppEncryptPropertyPattern()} だけが呼ばれる想定の、最小限の
     * {@link FessConfig} スタブを作る。フル実装（数百メソッド）を用意する代わりに、
     * 呼ばれた場合にのみ値を返す動的プロキシを使う。
     *
     * @param pattern {@link FessConfig#getAppEncryptPropertyPattern()} が返す値
     * @return 指定した値だけを返す {@link FessConfig}
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
