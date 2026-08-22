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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.ds.json.JsonRecordReader.Format;
import org.codelibs.fess.exception.DataStoreException;
import org.junit.jupiter.api.Test;

public class JsonRecordReaderTest {

    private List<Map<String, Object>> readAll(final String json, final Format format, final String rootPath) throws IOException {
        final InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        final List<Map<String, Object>> list = new ArrayList<>();
        try (JsonRecordReader reader = new JsonRecordReader(in, "UTF-8", format, rootPath)) {
            while (reader.hasNext()) {
                list.add(reader.next());
            }
        }
        return list;
    }

    private List<Map<String, Object>> readAll(final String json) throws IOException {
        return readAll(json, Format.AUTO, null);
    }

    @Test
    public void test_jsonl() throws IOException {
        final List<Map<String, Object>> list = readAll("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        assertEquals(3, list.size());
        assertEquals(1, list.get(0).get("a"));
        assertEquals(3, list.get(2).get("a"));
    }

    @Test
    public void test_jsonl_withBlankLines() throws IOException {
        final List<Map<String, Object>> list = readAll("{\"a\":1}\n\n   \n{\"a\":2}\n\n");
        assertEquals(2, list.size());
    }

    @Test
    public void test_jsonArray_oneLine() throws IOException {
        final List<Map<String, Object>> list = readAll("[{\"a\":1},{\"a\":2}]");
        assertEquals(2, list.size());
        assertEquals(2, list.get(1).get("a"));
    }

    @Test
    public void test_jsonArray_prettyPrinted() throws IOException {
        final List<Map<String, Object>> list = readAll("[\n  {\n    \"a\": 1\n  },\n  {\n    \"a\": 2\n  }\n]\n");
        assertEquals(2, list.size());
    }

    @Test
    public void test_singleObject_prettyPrinted() throws IOException {
        final List<Map<String, Object>> list = readAll("{\n  \"a\": 1,\n  \"b\": {\n    \"c\": 2\n  }\n}\n");
        assertEquals(1, list.size());
        assertTrue(list.get(0).get("b") instanceof Map);
    }

    @Test
    public void test_emptyArray() throws IOException {
        assertEquals(0, readAll("[]").size());
    }

    @Test
    public void test_emptyInput() throws IOException {
        assertEquals(0, readAll("").size());
        assertEquals(0, readAll("   \n  \n").size());
    }

    @Test
    public void test_arrayOfScalars_isRejected() {
        final DataStoreException e = assertThrows(DataStoreException.class, () -> readAll("[1,2,3]"));
        assertTrue(e.getMessage().contains("object"), "message should explain that objects are required: " + e.getMessage());
    }

    @Test
    public void test_utf8Bom_isStripped() throws IOException {
        final List<Map<String, Object>> list = readAll("﻿{\"a\":1}\n{\"a\":2}\n");
        assertEquals(2, list.size());
    }

    @Test
    public void test_format_jsonl_isNotAutoDetected() throws IOException {
        // Forcing JSONL on an array must fail rather than silently succeed.
        assertThrows(DataStoreException.class, () -> readAll("[{\"a\":1}]", Format.JSONL, null));
    }

    @Test
    public void test_parseFormat() {
        assertEquals(Format.AUTO, JsonRecordReader.parseFormat(null));
        assertEquals(Format.AUTO, JsonRecordReader.parseFormat("auto"));
        assertEquals(Format.JSONL, JsonRecordReader.parseFormat("JSONL"));
        assertEquals(Format.JSON, JsonRecordReader.parseFormat("json"));
        assertThrows(DataStoreException.class, () -> JsonRecordReader.parseFormat("yaml"));
    }
}
