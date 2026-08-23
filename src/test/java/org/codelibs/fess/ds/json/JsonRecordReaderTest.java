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
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    private JsonRecordReader openReader(final String json, final Format format, final String rootPath) throws IOException {
        return new JsonRecordReader(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), "UTF-8", format, rootPath);
    }

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

    /**
     * Reads every record, substituting the failure itself for a record that could not be parsed,
     * so a test can assert on the records and the failures between them in one list.
     */
    private List<Object> readOutcomes(final String json, final Format format, final String rootPath) throws IOException {
        final List<Object> outcomes = new ArrayList<>();
        try (JsonRecordReader reader = openReader(json, format, rootPath)) {
            while (reader.hasNext()) {
                try {
                    outcomes.add(reader.next());
                } catch (final DataStoreException e) {
                    outcomes.add(e);
                }
            }
        }
        return outcomes;
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
        final List<Map<String, Object>> list = readAll("\uFEFF{\"a\":1}\n{\"a\":2}\n");
        assertEquals(2, list.size());
    }

    @Test
    public void test_format_jsonl_isNotAutoDetected() throws IOException {
        // Forcing JSONL on an array must fail rather than silently succeed.
        assertThrows(DataStoreException.class, () -> readAll("[{\"a\":1}]", Format.JSONL, null));
    }

    /**
     * AUTO must pick the read strategy from the document's grammar: line by line for JSON
     * Lines, token stream for a single object spanning lines. Getting this wrong either
     * shreds a pretty-printed object into unparseable lines, or drags JSON Lines back onto a
     * token stream, where one bad line can swallow the record after it.
     */
    @Test
    public void test_auto_picksLinePathForJsonl_andTokenPathForPrettyPrintedObject() throws IOException {
        final String jsonl = "{\"a\":1}\nnot json\n{\"a\":3}\n";
        try (JsonRecordReader reader = openReader(jsonl, Format.AUTO, null)) {
            assertTrue(reader.isLineOriented(), "a document whose first non-blank line is a complete object is JSON Lines");
        }
        final List<Object> outcomes = readOutcomes(jsonl, Format.AUTO, null);
        assertEquals(3, outcomes.size(), "one outcome per non-blank line: " + outcomes);
        assertEquals(1, ((Map<?, ?>) outcomes.get(0)).get("a"));
        assertTrue(outcomes.get(1) instanceof DataStoreException, "the malformed line fails on its own: " + outcomes.get(1));
        assertEquals(3, ((Map<?, ?>) outcomes.get(2)).get("a"), "a malformed line must not cost the lines after it");

        final String prettyObject = "{\n  \"a\": 1,\n  \"b\": {\n    \"c\": 2\n  }\n}\n";
        try (JsonRecordReader reader = openReader(prettyObject, Format.AUTO, null)) {
            assertFalse(reader.isLineOriented(), "a single object spanning several lines is not line-delimited");
        }
        final List<Map<String, Object>> records = readAll(prettyObject);
        assertEquals(1, records.size(), "the token stream reads the whole object: " + records);
        assertTrue(records.get(0).get("b") instanceof Map, "the nested object survives, so the document was not read line by line");
    }

    /**
     * A minified document's "first line" is the whole file. The look-ahead must give up at its
     * limit and hand the document to the token stream rather than buffer it looking for a line
     * break that never comes.
     */
    @Test
    public void test_auto_documentWithNoLineBreakIsNotBuffered() throws IOException {
        final StringBuilder json = new StringBuilder("{");
        for (int i = 0; json.length() < 300000; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append("\"k").append(i).append("\":").append(i);
        }
        final String minified = json.append('}').toString();

        try (JsonRecordReader reader = openReader(minified, Format.AUTO, null)) {
            assertFalse(reader.isLineOriented(), "a document with no line break is read as a token stream");
        }
        final List<Map<String, Object>> records = readAll(minified);
        assertEquals(1, records.size());
        assertEquals(0, records.get(0).get("k0"));
    }

    /**
     * A broken first line must not be mistaken for the opening of a pretty-printed object: the
     * second non-blank line settles which it is.
     */
    @Test
    public void test_auto_malformedFirstLineIsStillJsonLines() throws IOException {
        assertTrue(isLineOriented("not json\n{\"a\":2}\n{\"a\":3}\n"), "a stray log line above real records is JSON Lines");
        assertTrue(isLineOriented("{not valid json\n{\"a\":2}\n"), "a broken record above a real one is JSON Lines");
        assertTrue(isLineOriented("{\"a\":\n{\"a\":2}\n"), "a record cut off mid-download is JSON Lines");
        assertFalse(isLineOriented("{\n  \"a\": 1,\n  \"b\": 2\n}\n"), "a pretty-printed object is not");
    }

    /**
     * Two broken lines above the records are as ordinary as one - a banner, or two lines of
     * transfer progress - so the look-ahead must not stop after the second. It reads on until a
     * line stands alone as a whole record.
     */
    @Test
    public void test_auto_severalMalformedLeadingLinesAreStillJsonLines() throws IOException {
        assertTrue(isLineOriented("garbage one\ngarbage two\n{\"a\":3}\n{\"a\":4}\n"), "a two-line banner above records is JSON Lines");
        assertTrue(isLineOriented("g1\ng2\ng3\ng4\ng5\n{\"a\":6}\n"), "and a five-line one is too");

        final List<Object> outcomes = readOutcomes("garbage one\ngarbage two\n{\"a\":3}\n{\"a\":4}\n", Format.AUTO, null);
        assertEquals(4, outcomes.size(), "one outcome per non-blank line: " + outcomes);
        assertTrue(outcomes.get(0) instanceof DataStoreException, "first banner line fails on its own: " + outcomes.get(0));
        assertTrue(outcomes.get(1) instanceof DataStoreException, "second banner line fails on its own: " + outcomes.get(1));
        assertEquals(3, ((Map<?, ?>) outcomes.get(2)).get("a"), "the records below the banner survive");
        assertEquals(4, ((Map<?, ?>) outcomes.get(3)).get("a"), "all of them");
    }

    /**
     * The scan that recovers from broken leading lines must not drag a pretty-printed document
     * onto the line path, however many lines it has: every line after the first is parsed
     * strictly, and a pretty-printed object has no line that stands alone as a whole object.
     */
    @Test
    public void test_auto_prettyPrintedObjectWithManyLinesIsTokenStream() throws IOException {
        final StringBuilder pretty = new StringBuilder("{\n");
        for (int i = 0; i < 200; i++) {
            pretty.append("  \"k").append(i).append("\": ").append(i).append(",\n");
        }
        final String json = pretty.append("  \"last\": true\n}\n").toString();

        assertFalse(isLineOriented(json), "a 202-line pretty-printed object is not line-delimited");
        final List<Map<String, Object>> records = readAll(json);
        assertEquals(1, records.size(), "the token stream reads it as one record: " + records.size());
        assertEquals(0, records.get(0).get("k0"));
        assertEquals(Boolean.TRUE, records.get(0).get("last"));
    }

    /**
     * A complete object indented onto its own line is a fragment of a record, not a record: the
     * last element of a nested array is written exactly that way, with no comma after it, so
     * strictness alone would accept it. Requiring the line to start at column zero rules it out.
     */
    @Test
    public void test_auto_indentedCompleteObjectIsNotARecord() throws IOException {
        final String json = "{\n  \"items\": [\n    {\"a\":1},\n    {\"a\":2}\n  ]\n}\n";

        assertFalse(isLineOriented(json), "a wrapper object holding minified array elements is not line-delimited");
        final List<Map<String, Object>> records = readAll(json);
        assertEquals(1, records.size(), "the whole wrapper is one record: " + records);
        assertTrue(records.get(0).get("items") instanceof List, "the nested array survives: " + records.get(0));
    }

    /**
     * A document that never offers a parseable line must fail cleanly on the token stream rather
     * than be scanned for one indefinitely.
     */
    @Test
    public void test_auto_allMalformedDocumentFailsCleanlyOnTheTokenStream() {
        assertThrows(IOException.class, () -> readAll("garbage one\ngarbage two\ngarbage three\n"),
                "nothing here stands alone as a record, so the token stream reads it and rejects the first token");
    }

    /**
     * The recovery scan is bounded: it looks a fixed number of lines down, not through the whole
     * document. A file of one-character junk lines is otherwise tens of thousands of failed parses
     * before the reader has decided anything, once per source.
     */
    @Test
    public void test_auto_recoveryScanIsBounded() throws IOException {
        assertTrue(isLineOriented(junkLines(63) + "{\"a\":1}\n"), "a record 63 lines down is still found");
        assertThrows(IOException.class, () -> readAll(junkLines(64) + "{\"a\":1}\n"),
                "one line further down is past the bound, so the document goes to the token stream");
    }

    private String junkLines(final int count) {
        final StringBuilder junk = new StringBuilder();
        for (int i = 0; i < count; i++) {
            junk.append("x\n");
        }
        return junk.toString();
    }

    /**
     * A file written with lone {@code '\r'} terminators must yield every record, not just the
     * first. Without CR handling the whole file is one line, the lenient first-line parse accepts
     * it, and every record after the first is dropped with no failure recorded at all.
     */
    @Test
    public void test_crOnlyLineEndings() throws IOException {
        final List<Map<String, Object>> list = readAll("{\"a\":1}\r{\"a\":2}\r{\"a\":3}\r");
        assertEquals(3, list.size(), "every CR-terminated record is read: " + list);
        assertEquals(1, list.get(0).get("a"));
        assertEquals(3, list.get(2).get("a"));

        final List<Map<String, Object>> forced = readAll("{\"a\":1}\r{\"a\":2}\r", Format.JSONL, null);
        assertEquals(2, forced.size(), "format=jsonl reads them too: " + forced);
    }

    /**
     * {@code "\r\n"} is one terminator, not two: joining the pair keeps a CRLF file from gaining
     * a blank line between every record, which would shift every line number after the first.
     */
    @Test
    public void test_crlfLineEndings_haveNoPhantomBlankLines() throws IOException {
        final String json = "{\"a\":1}\r\n{\"a\":2}\r\n{\"a\":3}\r\n";
        final List<Map<String, Object>> list = readAll(json);
        assertEquals(3, list.size(), "every CRLF-terminated record is read: " + list);

        final List<Integer> lineNumbers = new ArrayList<>();
        try (JsonRecordReader reader = openReader(json, Format.AUTO, null)) {
            while (reader.hasNext()) {
                reader.next();
                lineNumbers.add(reader.getCurrentLineNumber());
            }
        }
        assertEquals(List.of(1, 2, 3), lineNumbers, "records sit on lines 1, 2 and 3, not 1, 3 and 5");
    }

    /**
     * Line numbers must count CR-terminated lines the way an editor does.
     */
    @Test
    public void test_getCurrentLineNumber_crOnly() throws IOException {
        final List<Integer> lineNumbers = new ArrayList<>();
        try (JsonRecordReader reader = openReader("{\"a\":1}\r{\"a\":2}\r{\"a\":3}\r", Format.AUTO, null)) {
            while (reader.hasNext()) {
                reader.next();
                lineNumbers.add(reader.getCurrentLineNumber());
            }
        }
        assertEquals(List.of(1, 2, 3), lineNumbers);
    }

    private boolean isLineOriented(final String json) throws IOException {
        try (JsonRecordReader reader = openReader(json, Format.AUTO, null)) {
            return reader.isLineOriented();
        }
    }

    @Test
    public void test_parseFormat() {
        assertEquals(Format.AUTO, JsonRecordReader.parseFormat(null));
        assertEquals(Format.AUTO, JsonRecordReader.parseFormat("auto"));
        assertEquals(Format.JSONL, JsonRecordReader.parseFormat("JSONL"));
        assertEquals(Format.JSON, JsonRecordReader.parseFormat("json"));
        assertThrows(DataStoreException.class, () -> JsonRecordReader.parseFormat("yaml"));
    }

    @Test
    public void test_rootPath_nestedArray() throws IOException {
        final String json = "{\"meta\":{\"n\":2},\"data\":{\"items\":[{\"a\":1},{\"a\":2}]},\"tail\":9}";
        final List<Map<String, Object>> list = readAll(json, Format.AUTO, "/data/items");
        assertEquals(2, list.size());
        assertEquals(1, list.get(0).get("a"));
        assertEquals(2, list.get(1).get("a"));
    }

    @Test
    public void test_rootPath_topLevelArray() throws IOException {
        final String json = "{\"items\":[{\"a\":1}]}";
        assertEquals(1, readAll(json, Format.AUTO, "/items").size());
    }

    @Test
    public void test_rootPath_pointsAtObject() throws IOException {
        final String json = "{\"data\":{\"a\":1}}";
        final List<Map<String, Object>> list = readAll(json, Format.AUTO, "/data");
        assertEquals(1, list.size());
        assertEquals(1, list.get(0).get("a"));
    }

    @Test
    public void test_rootPath_notFound_yieldsNoRecords() throws IOException {
        final String json = "{\"data\":{\"items\":[{\"a\":1}]}}";
        assertEquals(0, readAll(json, Format.AUTO, "/missing/path").size());
    }

    @Test
    public void test_rootPath_emptyNestedArray() throws IOException {
        assertEquals(0, readAll("{\"data\":{\"items\":[]}}", Format.AUTO, "/data/items").size());
    }

    @Test
    public void test_getCurrentLineNumber_beforeFirstRecord() throws IOException {
        final InputStream in = new ByteArrayInputStream("{\"a\":1}\n".getBytes(StandardCharsets.UTF_8));
        try (JsonRecordReader reader = new JsonRecordReader(in, "UTF-8", Format.AUTO, null)) {
            assertEquals(-1, reader.getCurrentLineNumber());
        }
    }

    @Test
    public void test_getCurrentLineNumber_jsonl() throws IOException {
        final InputStream in = new ByteArrayInputStream("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n".getBytes(StandardCharsets.UTF_8));
        final List<Integer> lineNumbers = new ArrayList<>();
        try (JsonRecordReader reader = new JsonRecordReader(in, "UTF-8", Format.AUTO, null)) {
            while (reader.hasNext()) {
                reader.next();
                lineNumbers.add(reader.getCurrentLineNumber());
            }
        }
        // Records sit on lines 1, 2 and 3.
        assertEquals(List.of(1, 2, 3), lineNumbers);
    }

    @Test
    public void test_getCurrentLineNumber_skipsBlankLine() throws IOException {
        final InputStream in = new ByteArrayInputStream("{\"a\":1}\n\n{\"a\":2}\n".getBytes(StandardCharsets.UTF_8));
        final List<Integer> lineNumbers = new ArrayList<>();
        try (JsonRecordReader reader = new JsonRecordReader(in, "UTF-8", Format.AUTO, null)) {
            while (reader.hasNext()) {
                reader.next();
                lineNumbers.add(reader.getCurrentLineNumber());
            }
        }
        // Line 2 is blank; the second record actually begins on line 3.
        assertEquals(List.of(1, 3), lineNumbers);
    }

    @Test
    public void test_getCurrentLineNumber_prettyPrintedArray() throws IOException {
        final String json = "[\n  {\n    \"a\": 1\n  },\n  {\n    \"a\": 2\n  }\n]\n";
        final List<Integer> lineNumbers = new ArrayList<>();
        final InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        try (JsonRecordReader reader = new JsonRecordReader(in, "UTF-8", Format.AUTO, null)) {
            while (reader.hasNext()) {
                reader.next();
                lineNumbers.add(reader.getCurrentLineNumber());
            }
        }
        // The first object starts on line 2, the second on line 5.
        assertEquals(List.of(2, 5), lineNumbers);
    }

    @Test
    public void test_constructorFailure_closesStream_formatJsonlOnArray() {
        final TrackingInputStream in = new TrackingInputStream("[{\"a\":1}]".getBytes(StandardCharsets.UTF_8));
        assertThrows(DataStoreException.class, () -> new JsonRecordReader(in, "UTF-8", Format.JSONL, null));
        assertTrue(in.isClosed(), "stream must be closed when format=jsonl is rejected on an array");
    }

    @Test
    public void test_constructorFailure_closesStream_malformedJson() {
        // Invalid from the very first character, so parser.nextToken() throws inside the
        // constructor itself rather than later during iteration.
        final TrackingInputStream in = new TrackingInputStream("]not json at all".getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> new JsonRecordReader(in, "UTF-8", Format.AUTO, null));
        assertTrue(in.isClosed(), "stream must be closed when the parser fails on the first token");
    }

    @Test
    public void test_constructorFailure_closesStream_unsupportedEncoding() {
        final TrackingInputStream in = new TrackingInputStream("{\"a\":1}".getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> new JsonRecordReader(in, "totally-bogus-encoding", Format.AUTO, null));
        assertTrue(in.isClosed(), "stream must be closed when the requested encoding is unsupported");
    }

    @Test
    public void test_constructorFailure_closesStream_readThrows() {
        final TrackingInputStream in = new TrackingInputStream(new byte[0], true);
        assertThrows(IOException.class, () -> new JsonRecordReader(in, "UTF-8", Format.AUTO, null));
        assertTrue(in.isClosed(), "stream must be closed when the underlying stream fails on its first read");
    }

    /**
     * An {@link InputStream} that records whether {@link #close()} was called, and can
     * optionally fail on its first {@link #read()} to simulate a broken underlying source.
     */
    /**
     * A line holding only {@code null} is not a record. Jackson deserialises a bare JSON null
     * into a null Map instead of failing, so it was the one non-object line that reached the
     * caller; a line holding {@code true} or a number already failed here.
     */
    @Test
    public void test_jsonl_nullLineIsRejectedLikeAnyOtherNonObject() {
        final DataStoreException e = assertThrows(DataStoreException.class, () -> readAll("{\"a\":1}\nnull\n{\"b\":2}\n"),
                "a bare null must fail the line rather than yield a null record");
        assertTrue(e.getMessage().contains("line 2"), "the failing line is named: " + e.getMessage());
    }

    private static final class TrackingInputStream extends InputStream {

        private final byte[] data;

        private final boolean failOnRead;

        private int pos;

        private boolean closed;

        TrackingInputStream(final byte[] data) {
            this(data, false);
        }

        TrackingInputStream(final byte[] data, final boolean failOnRead) {
            this.data = data;
            this.failOnRead = failOnRead;
        }

        @Override
        public int read() throws IOException {
            if (failOnRead) {
                throw new IOException("simulated read failure");
            }
            return pos < data.length ? data[pos++] & 0xff : -1;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }

        boolean isClosed() {
            return closed;
        }
    }
}
