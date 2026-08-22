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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.exception.DataStoreException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

/**
 * Reads JSON records from a stream, one {@link Map} per record.
 *
 * <p>
 * Three shapes are supported and, with {@link Format#AUTO}, told apart by the first token:
 * </p>
 * <ul>
 * <li>JSON Lines - a sequence of objects, conventionally one per line</li>
 * <li>a JSON array of objects, however it is formatted</li>
 * <li>a single JSON object</li>
 * </ul>
 *
 * <p>
 * The whole document is never held in memory: records are pulled from the parser one at a time.
 * </p>
 */
public class JsonRecordReader implements java.util.Iterator<Map<String, Object>>, Closeable {

    /** Zero-width no-break space, i.e. the character a UTF-8 BOM decodes to. */
    private static final char BOM_CHAR = '﻿';

    /** Reused across every record; never reconfigured, which is Jackson's condition for sharing a mapper. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** The document shape this reader expects. */
    public enum Format {
        /** Detect the shape from the first token. */
        AUTO,
        /** A sequence of JSON objects. */
        JSONL,
        /** A JSON array of objects, or a single JSON object. */
        JSON
    }

    private final Reader reader;

    private final JsonParser parser;

    private final MappingIterator<Map<String, Object>> iterator;

    /**
     * True when {@code rootPath} pointed at a single object rather than an array, so this reader
     * must yield at most one record.
     *
     * <p>
     * {@code readValues} cannot tell a single nested object apart from an unwrapped sequence of
     * top-level objects: after the object's closing brace it would otherwise keep reading
     * whatever token follows in the enclosing document (a sibling field, or the parent's own
     * closing brace) as if it were another record. This flag caps that at one.
     * </p>
     */
    private final boolean singleRecordOnly;

    /** Whether the sole record has already been returned, when {@link #singleRecordOnly} is true. */
    private boolean singleRecordConsumed;

    /** The 1-based line where the most recently returned record began, or {@code -1} before the first record is read. */
    private int currentLineNumber = -1;

    /**
     * Opens a reader over the given stream.
     *
     * @param in the stream to read; closed by {@link #close()}
     * @param encoding the character encoding
     * @param format the expected shape
     * @param rootPath a JSON Pointer selecting a nested array, or {@code null} for the document root
     * @throws IOException if the stream cannot be read
     */
    @SuppressWarnings("unchecked")
    public JsonRecordReader(final InputStream in, final String encoding, final Format format, final String rootPath) throws IOException {
        final Reader openedReader = stripBom(new InputStreamReader(in, encoding));
        JsonParser openedParser = null;
        final MappingIterator<Map<String, Object>> openedIterator;
        boolean openedSingleRecordOnly = false;
        try {
            openedParser = objectMapper.getFactory().createParser(openedReader);

            JsonToken token = openedParser.nextToken();
            if (token == null) {
                // An empty document has no records.
                openedIterator = null;
            } else {
                final boolean nested = StringUtil.isNotBlank(rootPath);
                if (nested) {
                    token = navigateTo(openedParser, rootPath);
                }
                if (token == null) {
                    // The pointer did not match anything in the document.
                    openedIterator = null;
                } else if (token == JsonToken.START_ARRAY) {
                    if (format == Format.JSONL) {
                        throw new DataStoreException("format=jsonl was requested but the document starts with an array.");
                    }
                    // MappingIterator does not unwrap a root array: step inside it first.
                    if (openedParser.nextToken() == JsonToken.END_ARRAY) {
                        // An empty array has no records.
                        openedIterator = null;
                    } else {
                        openedIterator = (MappingIterator<Map<String, Object>>) (MappingIterator<?>) objectMapper.readValues(openedParser,
                                Map.class);
                    }
                } else if (token != JsonToken.START_OBJECT) {
                    throw new DataStoreException("Expected a JSON object or an array of objects but found " + token + ".");
                } else {
                    // A rootPath that lands on a bare object yields exactly that one record; see
                    // singleRecordOnly. Without rootPath, a bare object is either the sole
                    // top-level value in the document or the first of a JSONL sequence, and either
                    // way readValues() correctly stops at end-of-input.
                    openedSingleRecordOnly = nested;
                    openedIterator =
                            (MappingIterator<Map<String, Object>>) (MappingIterator<?>) objectMapper.readValues(openedParser, Map.class);
                }
            }
        } catch (final RuntimeException | IOException e) {
            // The reader/parser were opened above but this instance never finishes constructing,
            // so the caller's try-with-resources has nothing to close them with. Close them here.
            closeQuietly(openedParser, openedReader);
            throw e;
        }
        reader = openedReader;
        parser = openedParser;
        iterator = openedIterator;
        singleRecordOnly = openedSingleRecordOnly;
    }

    /**
     * Advances the given parser to the value named by the given JSON Pointer.
     *
     * @param p the parser to advance
     * @param rootPath the pointer
     * @return the token the parser is positioned on, or {@code null} if the pointer was not found
     * @throws IOException if the stream cannot be read
     */
    private static JsonToken navigateTo(final JsonParser p, final String rootPath) throws IOException {
        final JsonPointer pointer = JsonPointer.compile(rootPath);
        JsonToken token = p.currentToken();
        while (token != null) {
            // A FIELD_NAME shares the pointer of the value that follows it, so skip past it
            // to land on the value itself.
            if (token != JsonToken.FIELD_NAME && pointer.equals(p.getParsingContext().pathAsPointer())) {
                return token;
            }
            token = p.nextToken();
        }
        return null;
    }

    /**
     * Wraps the reader so that a leading byte order mark is discarded.
     *
     * <p>
     * {@link InputStreamReader} decodes a UTF-8 BOM to U+FEFF and hands it to the caller
     * rather than dropping it, which would make the first record unparseable.
     * </p>
     *
     * @param in the reader to wrap
     * @return a reader positioned after any BOM
     * @throws IOException if the stream cannot be read
     */
    private static Reader stripBom(final Reader in) throws IOException {
        final PushbackReader pushback = new PushbackReader(in, 1);
        final int first = pushback.read();
        if (first != -1 && first != BOM_CHAR) {
            pushback.unread(first);
        }
        return pushback;
    }

    /**
     * Closes a parser and/or reader that were opened while this instance was still under
     * construction, ignoring any error encountered while closing them.
     *
     * @param p the parser to close, may be {@code null} if not yet created
     * @param r the reader to close, may be {@code null}
     */
    private static void closeQuietly(final JsonParser p, final Reader r) {
        try {
            if (p != null) {
                p.close();
            }
        } catch (final IOException ignore) {
            // best-effort cleanup; the exception that triggered this close is what matters
        } finally {
            try {
                if (r != null) {
                    r.close();
                }
            } catch (final IOException ignore) {
                // best-effort cleanup; the exception that triggered this close is what matters
            }
        }
    }

    /**
     * Parses the {@code format} parameter value.
     *
     * @param value the parameter value, may be {@code null} or blank
     * @return the matching format, {@link Format#AUTO} when unset
     */
    public static Format parseFormat(final String value) {
        if (StringUtil.isBlank(value)) {
            return Format.AUTO;
        }
        try {
            return Format.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            throw new DataStoreException("Unknown format: " + value + ". Expected auto, jsonl or json.", e);
        }
    }

    @Override
    public boolean hasNext() {
        if (iterator == null || (singleRecordOnly && singleRecordConsumed)) {
            return false;
        }
        try {
            return iterator.hasNext();
        } catch (final RuntimeJsonMappingException e) {
            throw new DataStoreException("Expected a JSON object but found a different value: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> next() {
        if (iterator == null || (singleRecordOnly && singleRecordConsumed)) {
            throw new NoSuchElementException();
        }
        try {
            if (!iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            // Capture the location of the token that starts this record - i.e. the parser's
            // current position - before next() consumes it and moves the parser past it.
            currentLineNumber = parser.currentTokenLocation().getLineNr();
            final Map<String, Object> value = iterator.next();
            singleRecordConsumed = true;
            return value;
        } catch (final RuntimeJsonMappingException e) {
            throw new DataStoreException("Expected a JSON object but found a different value: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the line in the source where the most recently returned record began.
     *
     * <p>
     * Used to report a failing record at the line a human would find in an editor, which stays
     * meaningful for pretty-printed documents where one record spans many lines.
     * </p>
     *
     * @return a 1-based line number, or {@code -1} before the first record is read
     */
    public int getCurrentLineNumber() {
        return currentLineNumber;
    }

    @Override
    public void close() throws IOException {
        try {
            if (iterator != null) {
                iterator.close();
            }
        } finally {
            try {
                parser.close();
            } finally {
                reader.close();
            }
        }
    }
}
