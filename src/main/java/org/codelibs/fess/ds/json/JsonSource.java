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

/**
 * One unit of input for the JSON data store.
 *
 * <p>
 * An implementation knows how to reach its bytes and nothing about how they are parsed.
 * </p>
 */
public interface JsonSource extends Closeable {

    /**
     * Returns the identifier used in logs, crawler statistics and failure records.
     *
     * @return an absolute file path or a URL
     */
    String getName();

    /**
     * Returns when this source last changed.
     *
     * @return epoch milliseconds, or {@code 0} when unknown
     */
    long getLastModified();

    /**
     * Opens the content stream. Called at most once per source.
     *
     * @return the content
     * @throws IOException if the source cannot be read
     */
    InputStream openStream() throws IOException;
}
