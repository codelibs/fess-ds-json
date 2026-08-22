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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link JsonSource} backed by a file on the local file system.
 */
public class FileJsonSource implements JsonSource {

    private final File file;

    /**
     * Creates a source for the given file.
     *
     * @param file the file to read
     */
    public FileJsonSource(final File file) {
        this.file = file;
    }

    @Override
    public String getName() {
        return file.getAbsolutePath();
    }

    @Override
    public long getLastModified() {
        return file.lastModified();
    }

    @Override
    public InputStream openStream() throws IOException {
        return new FileInputStream(file);
    }

    @Override
    public void close() {
        // The stream returned by openStream() is owned by the caller.
    }
}
