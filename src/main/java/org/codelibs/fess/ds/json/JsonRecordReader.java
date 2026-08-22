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

import java.io.BufferedReader;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

/**
 * Reads JSON records from a stream, one {@link Map} per record.
 *
 * <p>
 * Three shapes are supported and, with {@link Format#AUTO}, told apart by the document's own
 * grammar:
 * </p>
 * <ul>
 * <li>JSON Lines - a sequence of objects, one per line</li>
 * <li>a JSON array of objects, however it is formatted</li>
 * <li>a single JSON object, however it is formatted</li>
 * </ul>
 *
 * <p>
 * Those shapes are not read the same way, because they are not delimited the same way. JSON
 * Lines is line-delimited by definition, so it is read one line at a time and each line is
 * parsed on its own: a malformed line costs exactly that line, and the line after it starts a
 * fresh, independent parse. An array or a single object is not line-delimited, so it is read
 * through a Jackson token stream, which is also what lets a huge array and a {@code rootPath}
 * pointer be read without holding the document in memory. A token stream cannot give the same
 * per-record isolation: once a record fails to parse mid-token, Jackson resynchronizes by
 * stepping over characters, which can silently swallow the record that follows or, for a
 * document truncated mid-object, never resynchronize at all - so callers must bound how long
 * they keep reading a token stream that only produces failures.
 * </p>
 *
 * <p>
 * Neither path holds the whole document in memory: records are pulled one at a time. The
 * look-ahead that chooses between them buffers the first non-blank line, which a single record
 * has to fit in anyway.
 * </p>
 */
public class JsonRecordReader implements java.util.Iterator<Map<String, Object>>, Closeable {

    /** Zero-width no-break space, i.e. the character a UTF-8 BOM decodes to. */
    private static final char BOM_CHAR = '\uFEFF';

    /** Reused across every record; never reconfigured, which is Jackson's condition for sharing a mapper. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Reused across all records to avoid a per-record allocation for the same generic type. */
    private final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {
    };

    /** The document shape this reader expects. */
    public enum Format {
        /** Detect the shape from the document's grammar. */
        AUTO,
        /** A sequence of JSON objects, one per line, each parsed independently. */
        JSONL,
        /**
         * A JSON array of objects, or a single JSON object, read as a token stream. Not
         * enforced: a JSONL-shaped stream is still read in full rather than rejected or capped
         * at one record, since the first token alone cannot tell "one object" apart from
         * "many".
         */
        JSON
    }

    private final Reader reader;

    /** {@code null} when this reader parses the document one line at a time. */
    private final JsonParser parser;

    /** {@code null} when this reader parses line by line, or when the document has no records. */
    private final MappingIterator<Map<String, Object>> iterator;

    /** True when each record is parsed from its own line rather than from a token stream. */
    private final boolean lineOriented;

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
     * The next line to parse, already read from the stream, or {@code null} when none is
     * buffered. Line-oriented reading only.
     */
    private String pendingLine;

    /** How many lines have been read from the stream so far. Line-oriented reading only. */
    private int lineCounter;

    /**
     * A token-stream failure raised by {@link #hasNext()} and held back so that {@link #next()}
     * throws it instead. See {@link #hasNext()} for why.
     */
    private DataStoreException pendingFailure;

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
        Reader openedReader = null;
        JsonParser openedParser = null;
        MappingIterator<Map<String, Object>> openedIterator = null;
        boolean openedLineOriented = false;
        boolean openedSingleRecordOnly = false;
        String openedPendingLine = null;
        int openedLineCounter = 0;
        try {
            openedReader = new BufferedReader(stripBom(new InputStreamReader(in, encoding)));

            // Look ahead at the first non-blank line to see which grammar this document is
            // written in. Everything consumed here is kept so that whichever path is chosen
            // still sees the complete document, line breaks included.
            final StringBuilder consumed = new StringBuilder();
            String firstLine = null;
            int blankLines = 0;
            for (String raw; (raw = readRawLine(openedReader)) != null;) {
                consumed.append(raw);
                if (!raw.isBlank()) {
                    firstLine = raw;
                    break;
                }
                blankLines++;
            }

            if (isLineDelimited(format, rootPath, firstLine)) {
                openedLineOriented = true;
                // The look-ahead already read the first record; hand it straight to next().
                openedPendingLine = firstLine;
                openedLineCounter = blankLines + (firstLine != null ? 1 : 0);
            } else {
                if (consumed.length() > 0) {
                    // Put the look-ahead back in front of the rest of the stream so the parser
                    // reads the document from its very first character.
                    final PushbackReader pushback = new PushbackReader(openedReader, consumed.length());
                    pushback.unread(consumed.toString().toCharArray());
                    openedReader = pushback;
                }
                openedParser = objectMapper.getFactory().createParser(openedReader);

                JsonToken token = openedParser.nextToken();
                if (token != null) {
                    final boolean nested = StringUtil.isNotBlank(rootPath);
                    if (nested) {
                        token = navigateTo(openedParser, rootPath);
                    }
                    if (token == null) {
                        // The pointer did not match anything in the document.
                        openedIterator = null;
                    } else if (token == JsonToken.START_ARRAY) {
                        // MappingIterator does not unwrap a root array: step inside it first.
                        if (openedParser.nextToken() != JsonToken.END_ARRAY) {
                            openedIterator = (MappingIterator<Map<String, Object>>) (MappingIterator<?>) objectMapper
                                    .readValues(openedParser, Map.class);
                        }
                    } else if (token != JsonToken.START_OBJECT) {
                        throw new DataStoreException("Expected a JSON object or an array of objects but found " + token + ".");
                    } else {
                        // A rootPath that lands on a bare object yields exactly that one record;
                        // see singleRecordOnly. Without rootPath, a bare object here is a
                        // document the look-ahead decided is not line-delimited, and readValues()
                        // correctly stops at end-of-input.
                        openedSingleRecordOnly = nested;
                        openedIterator = (MappingIterator<Map<String, Object>>) (MappingIterator<?>) objectMapper.readValues(openedParser,
                                Map.class);
                    }
                }
            }
        } catch (final RuntimeException | IOException e) {
            // Whatever of the reader/parser/stream chain was opened above, this instance never
            // finishes constructing, so the caller's try-with-resources has nothing to close them
            // with - not even the InputStream passed in, since it is wrapped, not owned, until the
            // constructor returns successfully. Close them here instead.
            closeQuietly(openedParser, openedReader, in);
            throw e;
        }
        reader = openedReader;
        parser = openedParser;
        iterator = openedIterator;
        lineOriented = openedLineOriented;
        singleRecordOnly = openedSingleRecordOnly;
        pendingLine = openedPendingLine;
        lineCounter = openedLineCounter;
    }

    /**
     * Decides whether the document should be read one line at a time.
     *
     * <p>
     * {@link Format#JSONL} and {@link Format#JSON} say so outright; {@link Format#AUTO} decides
     * from the first non-whitespace character and, for a document starting with an object, from
     * whether the first non-blank line is a complete JSON object on its own. A line that parses
     * standalone means the document is line-delimited; a line that does not means the object
     * spans lines, which only a token stream can read. A {@code rootPath} always implies the
     * token stream, since a pointer only makes sense inside a structured document.
     * </p>
     *
     * @param format the requested format
     * @param rootPath a JSON Pointer selecting a nested value, may be {@code null} or blank
     * @param firstLine the first non-blank line of the document, or {@code null} if there is none
     * @return {@code true} to parse each line on its own, {@code false} to run a token stream
     */
    private boolean isLineDelimited(final Format format, final String rootPath, final String firstLine) {
        if (StringUtil.isNotBlank(rootPath) || format == Format.JSON) {
            return false;
        }
        final char first = firstNonWhitespace(firstLine);
        if (format == Format.JSONL) {
            if (first == '[') {
                throw new DataStoreException("format=jsonl was requested but the document starts with an array.");
            }
            return true;
        }
        if (first == '{') {
            return parsesAsCompleteObject(firstLine);
        }
        // An array, an empty document, or something that is neither: let the token stream read
        // it and report exactly the errors it has always reported.
        return false;
    }

    /**
     * Returns the first character of the given line that is not whitespace.
     *
     * @param line the line to inspect, may be {@code null}
     * @return that character, or {@code '\0'} when the line is {@code null} or all whitespace
     */
    private static char firstNonWhitespace(final String line) {
        if (line == null) {
            return '\0';
        }
        for (int i = 0; i < line.length(); i++) {
            final char c = line.charAt(i);
            if (!Character.isWhitespace(c)) {
                return c;
            }
        }
        return '\0';
    }

    /**
     * Reports whether the given line is a complete JSON object all by itself.
     *
     * <p>
     * This is what tells JSON Lines apart from a single pretty-printed object: the first line of
     * {@code {"a":1}\n{"a":2}} parses, the first line of {@code {\n  "a": 1\n}} ({@code "{"})
     * does not.
     * </p>
     *
     * <p>
     * Only the structure is walked: the look-ahead asks a yes/no question, so no Map is built
     * for a line that is read here and then read again as a record.
     * </p>
     *
     * @param line the line to try
     * @return {@code true} if the line parses on its own as a JSON object
     */
    private boolean parsesAsCompleteObject(final String line) {
        try (JsonParser lineParser = objectMapper.getFactory().createParser(line)) {
            if (lineParser.nextToken() != JsonToken.START_OBJECT) {
                return false;
            }
            lineParser.skipChildren();
            return true;
        } catch (final IOException e) {
            return false;
        }
    }

    /**
     * Reads one line, keeping every character exactly as it appeared, including the terminating
     * {@code '\n'}.
     *
     * <p>
     * {@link BufferedReader#readLine()} is not used because it discards the terminator and does
     * not say which one it discarded, and the token path needs the characters back verbatim for
     * its line numbers to match the file. A lone {@code '\r'} is treated as ordinary whitespace
     * rather than as a line terminator.
     * </p>
     *
     * @param in the reader to read from
     * @return the line, or {@code null} at end of stream
     * @throws IOException if the stream cannot be read
     */
    private static String readRawLine(final Reader in) throws IOException {
        final StringBuilder line = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            line.append((char) c);
            if (c == '\n') {
                break;
            }
        }
        return line.length() == 0 ? null : line.toString();
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
     * Closes a parser, reader and/or the raw stream that were opened while this instance was
     * still under construction, ignoring any error encountered while closing them.
     *
     * <p>
     * Closing {@code p} or {@code r} normally closes {@code in} too, since each wraps the one
     * before it, but a failure can strike before any of them exist - an unsupported encoding
     * name fails before {@code r} is built, for instance - so {@code in} is always closed
     * directly as well. {@link Closeable#close()} is specified to have no effect on an
     * already-closed stream, so closing it more than once here is not a problem.
     * </p>
     *
     * @param p the parser to close, may be {@code null} if not yet created
     * @param r the reader to close, may be {@code null} if not yet created
     * @param in the raw stream passed to the constructor
     */
    private static void closeQuietly(final JsonParser p, final Reader r, final InputStream in) {
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
            } finally {
                try {
                    in.close();
                } catch (final IOException ignore) {
                    // best-effort cleanup; the exception that triggered this close is what matters
                }
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

    /**
     * Reports whether this reader parses each record from its own line.
     *
     * <p>
     * Callers need this to know how a failed record behaves: a line-oriented read always moves
     * on to the next line, whereas a token stream may keep failing on the same unparseable
     * remainder and has to be bounded by the caller.
     * </p>
     *
     * @return {@code true} when each record is parsed from its own line
     */
    public boolean isLineOriented() {
        return lineOriented;
    }

    @Override
    public boolean hasNext() {
        if (lineOriented) {
            return hasNextLine();
        }
        if (iterator == null || (singleRecordOnly && singleRecordConsumed)) {
            return false;
        }
        if (pendingFailure != null) {
            return true;
        }
        try {
            return iterator.hasNext();
        } catch (final RuntimeException e) {
            // MappingIterator wraps a parse failure raised while looking for the next record in
            // a plain RuntimeException. Thrown from here it would escape the caller's per-record
            // error handling, which can only guard next(). Hold it back instead and let next()
            // throw it, so every parse failure surfaces from one place and is reported against
            // one record.
            pendingFailure = toDataStoreException(e);
            return true;
        }
    }

    @Override
    public Map<String, Object> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (lineOriented) {
            return nextLine();
        }
        if (pendingFailure != null) {
            final DataStoreException failure = pendingFailure;
            pendingFailure = null;
            throw failure;
        }
        try {
            // Capture the location of the token that starts this record - i.e. the parser's
            // current position - before next() consumes it and moves the parser past it.
            currentLineNumber = parser.currentTokenLocation().getLineNr();
            final Map<String, Object> value = iterator.next();
            singleRecordConsumed = true;
            return value;
        } catch (final RuntimeException e) {
            throw toDataStoreException(e);
        }
    }

    /**
     * Buffers the next non-blank line, skipping blank ones.
     *
     * @return {@code true} if a line is buffered and ready for {@link #nextLine()}
     */
    private boolean hasNextLine() {
        if (pendingLine != null) {
            return true;
        }
        try {
            for (String raw; (raw = readRawLine(reader)) != null;) {
                lineCounter++;
                if (!raw.isBlank()) {
                    pendingLine = raw;
                    return true;
                }
            }
        } catch (final IOException e) {
            throw new DataStoreException("Failed to read line " + (lineCounter + 1) + ".", e);
        }
        return false;
    }

    /**
     * Parses the buffered line on its own.
     *
     * <p>
     * The line number is recorded before parsing so that a line which fails to parse is still
     * reported against the line it is on.
     * </p>
     *
     * @return the record the line holds, never {@code null}
     */
    private Map<String, Object> nextLine() {
        final String line = pendingLine;
        pendingLine = null;
        currentLineNumber = lineCounter;
        try {
            final Map<String, Object> record = objectMapper.readValue(line, mapTypeReference);
            if (record == null) {
                // Jackson turns a bare JSON null into a null Map rather than failing, so this is the
                // one non-object line the deserialiser lets through; every other scalar throws below.
                throw new DataStoreException("Expected a JSON object on line " + currentLineNumber + " but found null.");
            }
            return record;
        } catch (final IOException e) {
            throw new DataStoreException("Failed to parse the JSON object on line " + currentLineNumber + ".", e);
        }
    }

    /**
     * Converts a failure raised by the token stream into the exception type callers expect.
     *
     * @param e the failure {@link MappingIterator} raised
     * @return the equivalent {@link DataStoreException}
     */
    private static DataStoreException toDataStoreException(final RuntimeException e) {
        if (e instanceof RuntimeJsonMappingException) {
            return new DataStoreException("Expected a JSON object but found a different value: " + e.getMessage(), e);
        }
        return new DataStoreException("Failed to read the next JSON record: " + e.getMessage(), e);
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
                if (parser != null) {
                    parser.close();
                }
            } finally {
                reader.close();
            }
        }
    }
}
