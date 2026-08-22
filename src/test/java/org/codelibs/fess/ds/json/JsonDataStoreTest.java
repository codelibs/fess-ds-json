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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.CrawlingConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.opensearch.config.exentity.FailureUrl;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.json.UnitDsTestCase;

/**
 * Comprehensive unit tests for JsonDataStore class.
 * Tests cover file detection, encoding handling, JSON/JSONL processing,
 * file list management, and error scenarios.
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
     * Test that default file suffixes include .json and .jsonl.
     */
    @Test
    public void test_isDesiredFile_defaultSuffixes() throws Exception {
        File parentDir = new File("/tmp");

        // Test .json files
        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.json"));
        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "TEST.JSON"));
        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "data.Json"));

        // Test .jsonl files
        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.jsonl"));
        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "TEST.JSONL"));
        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "data.Jsonl"));

        // Test non-JSON files
        assertFalse(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.txt"));
        assertFalse(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.xml"));
        assertFalse(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.csv"));
        assertFalse(invokeMethod(dataStore, "isDesiredFile", parentDir, "testjson"));
    }

    /**
     * Test setting custom file suffixes.
     */
    @Test
    public void test_setFileSuffixes_customSuffixes() throws Exception {
        dataStore.setFileSuffixes(new String[] { ".data", ".txt" });
        File parentDir = new File("/tmp");

        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.data"));
        assertTrue(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.txt"));
        assertFalse(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.json"));
        assertFalse(invokeMethod(dataStore, "isDesiredFile", parentDir, "test.jsonl"));
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
     * Test getFileList throws exception when both files and directories are blank.
     */
    @Test
    public void test_getFileList_blankParameters() {
        DataStoreParams params = new DataStoreParams();

        try {
            invokeMethod(dataStore, "getFileList", params);
            fail("Expected DataStoreException");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // InvocationTargetException wraps the actual exception
            Throwable cause = e.getCause();
            assertTrue("Expected DataStoreException but got: " + cause.getClass().getName(), cause instanceof DataStoreException);
            assertTrue(cause.getMessage().contains("files") && cause.getMessage().contains("directories"));
        } catch (Exception e) {
            fail("Expected InvocationTargetException with DataStoreException but got: " + e.getClass().getName());
        }
    }

    /**
     * Test getFileList with files parameter.
     */
    @Test
    public void test_getFileList_withFilesParameter() throws Exception {
        // Create temporary test files
        Path tempDir = Files.createTempDirectory("jsontest");
        Path jsonFile1 = Files.createTempFile(tempDir, "test1", ".json");
        Path jsonFile2 = Files.createTempFile(tempDir, "test2", ".jsonl");
        Path txtFile = Files.createTempFile(tempDir, "test3", ".txt");

        try {
            Files.write(jsonFile1, "{\"test\": 1}".getBytes());
            Files.write(jsonFile2, "{\"test\": 2}".getBytes());
            Files.write(txtFile, "text".getBytes());

            DataStoreParams params = new DataStoreParams();
            params.put("files", jsonFile1.toString() + "," + jsonFile2.toString() + "," + txtFile.toString());

            Object result = invokeMethod(dataStore, "getFileList", params);
            assertNotNull(result);
            assertTrue(result instanceof java.util.List);

            @SuppressWarnings("unchecked")
            java.util.List<File> fileList = (java.util.List<File>) result;

            // Should only include .json and .jsonl files, not .txt
            assertEquals(2, fileList.size());
            assertTrue(fileList.stream().anyMatch(f -> f.getName().endsWith(".json")));
            assertTrue(fileList.stream().anyMatch(f -> f.getName().endsWith(".jsonl")));
            assertFalse(fileList.stream().anyMatch(f -> f.getName().endsWith(".txt")));

        } finally {
            // Clean up
            Files.deleteIfExists(jsonFile1);
            Files.deleteIfExists(jsonFile2);
            Files.deleteIfExists(txtFile);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * Test getFileList with directories parameter.
     */
    @Test
    public void test_getFileList_withDirectoriesParameter() throws Exception {
        // Create temporary directory with test files
        Path tempDir = Files.createTempDirectory("jsontest");
        Path jsonFile1 = Files.createTempFile(tempDir, "test1", ".json");
        Path jsonFile2 = Files.createTempFile(tempDir, "test2", ".jsonl");
        Path txtFile = Files.createTempFile(tempDir, "test3", ".txt");

        try {
            Files.write(jsonFile1, "{\"test\": 1}".getBytes());
            Files.write(jsonFile2, "{\"test\": 2}".getBytes());
            Files.write(txtFile, "text".getBytes());

            DataStoreParams params = new DataStoreParams();
            params.put("directories", tempDir.toString());

            Object result = invokeMethod(dataStore, "getFileList", params);
            assertNotNull(result);
            assertTrue(result instanceof java.util.List);

            @SuppressWarnings("unchecked")
            java.util.List<File> fileList = (java.util.List<File>) result;

            // Should only include .json and .jsonl files from directory
            assertEquals(2, fileList.size());
            assertTrue(fileList.stream().anyMatch(f -> f.getName().endsWith(".json")));
            assertTrue(fileList.stream().anyMatch(f -> f.getName().endsWith(".jsonl")));
            assertFalse(fileList.stream().anyMatch(f -> f.getName().endsWith(".txt")));

        } finally {
            // Clean up
            Files.deleteIfExists(jsonFile1);
            Files.deleteIfExists(jsonFile2);
            Files.deleteIfExists(txtFile);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * Test getFileList with multiple directories.
     */
    @Test
    public void test_getFileList_withMultipleDirectories() throws Exception {
        Path tempDir1 = Files.createTempDirectory("jsontest1");
        Path tempDir2 = Files.createTempDirectory("jsontest2");
        Path jsonFile1 = Files.createTempFile(tempDir1, "test1", ".json");
        Path jsonFile2 = Files.createTempFile(tempDir2, "test2", ".jsonl");

        try {
            Files.write(jsonFile1, "{\"test\": 1}".getBytes());
            Files.write(jsonFile2, "{\"test\": 2}".getBytes());

            DataStoreParams params = new DataStoreParams();
            params.put("directories", tempDir1.toString() + "," + tempDir2.toString());

            Object result = invokeMethod(dataStore, "getFileList", params);
            assertNotNull(result);

            @SuppressWarnings("unchecked")
            java.util.List<File> fileList = (java.util.List<File>) result;

            // Should include files from both directories
            assertEquals(2, fileList.size());

        } finally {
            // Clean up
            Files.deleteIfExists(jsonFile1);
            Files.deleteIfExists(jsonFile2);
            Files.deleteIfExists(tempDir1);
            Files.deleteIfExists(tempDir2);
        }
    }

    /**
     * Test getFileList with non-existent file path.
     */
    @Test
    public void test_getFileList_withNonExistentFile() throws Exception {
        DataStoreParams params = new DataStoreParams();
        params.put("files", "/nonexistent/path/test.json");

        Object result = invokeMethod(dataStore, "getFileList", params);
        assertNotNull(result);

        @SuppressWarnings("unchecked")
        java.util.List<File> fileList = (java.util.List<File>) result;

        // Should return empty list for non-existent files
        assertTrue(fileList.isEmpty());
    }

    /**
     * Test getFileList with non-directory path.
     */
    @Test
    public void test_getFileList_withNonDirectoryPath() throws Exception {
        Path tempFile = Files.createTempFile("notadir", ".json");

        try {
            Files.write(tempFile, "{\"test\": 1}".getBytes());

            DataStoreParams params = new DataStoreParams();
            params.put("directories", tempFile.toString());

            Object result = invokeMethod(dataStore, "getFileList", params);
            assertNotNull(result);

            @SuppressWarnings("unchecked")
            java.util.List<File> fileList = (java.util.List<File>) result;

            // Should return empty list when path is not a directory
            assertTrue(fileList.isEmpty());

        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /**
     * Test file sorting by last modified time.
     */
    @Test
    public void test_getFileList_sortedByModifiedTime() throws Exception {
        Path tempDir = Files.createTempDirectory("jsontest");
        Path jsonFile1 = Files.createTempFile(tempDir, "test1", ".json");
        Path jsonFile2 = Files.createTempFile(tempDir, "test2", ".json");
        Path jsonFile3 = Files.createTempFile(tempDir, "test3", ".json");

        try {
            // Write files and ensure different modification times
            Files.write(jsonFile1, "{\"id\": 1}".getBytes());
            Thread.sleep(100);
            Files.write(jsonFile2, "{\"id\": 2}".getBytes());
            Thread.sleep(100);
            Files.write(jsonFile3, "{\"id\": 3}".getBytes());

            DataStoreParams params = new DataStoreParams();
            params.put("directories", tempDir.toString());

            Object result = invokeMethod(dataStore, "getFileList", params);

            @SuppressWarnings("unchecked")
            java.util.List<File> fileList = (java.util.List<File>) result;

            assertEquals(3, fileList.size());

            // Files should be sorted by modification time (oldest first)
            long prevTime = 0;
            for (File f : fileList) {
                long currentTime = f.lastModified();
                assertTrue(currentTime >= prevTime);
                prevTime = currentTime;
            }

        } finally {
            Files.deleteIfExists(jsonFile1);
            Files.deleteIfExists(jsonFile2);
            Files.deleteIfExists(jsonFile3);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * カンマ区切りのパスに前後の空白があっても解決できることを検証する。
     */
    @Test
    public void test_getFileList_withFilesParameter_trimsWhitespace() throws Exception {
        final Path tempDir = Files.createTempDirectory("jsontrim");
        final Path a = Files.createFile(tempDir.resolve("a.json"));
        final Path b = Files.createFile(tempDir.resolve("b.jsonl"));

        try {
            final DataStoreParams params = new DataStoreParams();
            params.put("files", a + " , " + b);

            final Method method = JsonDataStore.class.getDeclaredMethod("getFileList", DataStoreParams.class);
            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            final List<File> fileList = (List<File>) method.invoke(dataStore, params);

            assertEquals("both paths resolve after trimming", 2, fileList.size());
        } finally {
            Files.deleteIfExists(a);
            Files.deleteIfExists(b);
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * ディレクトリ指定でも前後の空白と空要素を許容することを検証する。
     */
    @Test
    public void test_getFileList_withDirectoriesParameter_trimsWhitespace() throws Exception {
        final Path tempDir = Files.createTempDirectory("jsontrimdir");
        Files.createFile(tempDir.resolve("a.json"));

        try {
            final DataStoreParams params = new DataStoreParams();
            params.put("directories", " " + tempDir + " , ");

            final Method method = JsonDataStore.class.getDeclaredMethod("getFileList", DataStoreParams.class);
            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            final List<File> fileList = (List<File>) method.invoke(dataStore, params);

            assertEquals("whitespace-only element is skipped, directory resolves", 1, fileList.size());
        } finally {
            Files.deleteIfExists(tempDir.resolve("a.json"));
            Files.deleteIfExists(tempDir);
        }
    }

    /**
     * Test storeData with empty file list.
     */
    @Test
    public void test_storeData_emptyFileList() throws Exception {
        DataConfig dataConfig = new DataConfig();
        IndexUpdateCallback callback = new TestIndexUpdateCallback();
        DataStoreParams params = new DataStoreParams();
        Map<String, String> scriptMap = new HashMap<>();
        Map<String, Object> defaultDataMap = new HashMap<>();

        // Set invalid path to ensure empty file list
        params.put("files", "/nonexistent/path/test.json");

        // This should log a warning and return without processing
        dataStore.storeData(dataConfig, callback, params, scriptMap, defaultDataMap);

        // No exception should be thrown
        assertTrue(true);
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
     * Test that getFileList sorts files correctly even when the last-modified
     * gap between files exceeds Integer.MAX_VALUE milliseconds (~24.8 days),
     * which previously overflowed the truncated int comparator and reversed
     * the sort order.
     */
    @Test
    public void test_getFileList_sorting_beyond_int_overflow_threshold() throws Exception {
        final File tempDir = new File(System.getProperty("java.io.tmpdir"), "json_overflow_sort_test_" + System.nanoTime());
        tempDir.mkdirs();

        final long day = 24L * 60L * 60L * 1000L;
        final long base = System.currentTimeMillis() - 400L * day;
        final File oldest = new File(tempDir, "oldest.json");
        final File middle = new File(tempDir, "middle.json");
        final File newest = new File(tempDir, "newest.json");

        try {
            oldest.createNewFile();
            middle.createNewFile();
            newest.createNewFile();
            oldest.setLastModified(base);
            middle.setLastModified(base + 30L * day);
            newest.setLastModified(base + 60L * day);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", tempDir.getAbsolutePath());

            final Object result = invokeMethod(dataStore, "getFileList", params);

            @SuppressWarnings("unchecked")
            final java.util.List<File> fileList = (java.util.List<File>) result;

            assertEquals(3, fileList.size());
            assertEquals("oldest.json", fileList.get(0).getName());
            assertEquals("middle.json", fileList.get(1).getName());
            assertEquals("newest.json", fileList.get(2).getName());
        } finally {
            oldest.delete();
            middle.delete();
            newest.delete();
            tempDir.delete();
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
            // Handle primitive types
            if (paramTypes[i] == File.class && args[i].getClass() != File.class) {
                paramTypes[i] = File.class;
            }
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
