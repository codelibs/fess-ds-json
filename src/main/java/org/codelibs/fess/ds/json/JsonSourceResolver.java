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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;

/**
 * Builds the list of {@link JsonSource}s a crawl should read from its parameters.
 */
public class JsonSourceResolver {

    private static final Logger logger = LogManager.getLogger(JsonSourceResolver.class);

    /** Comma-separated file paths. */
    protected static final String FILES_PARAM = "files";

    /** Comma-separated directory paths. */
    protected static final String DIRS_PARAM = "directories";

    /** Comma-separated URLs. */
    protected static final String URLS_PARAM = "urls";

    /** Whether directories are walked recursively. */
    protected static final String RECURSIVE_PARAM = "recursive";

    /** How deep recursion may go below each directory. */
    protected static final String MAX_DEPTH_PARAM = "max_depth";

    /** Regular expression a source name must match. */
    protected static final String INCLUDE_PATTERN_PARAM = "include_pattern";

    /** Regular expression a source name must not match. */
    protected static final String EXCLUDE_PATTERN_PARAM = "exclude_pattern";

    /** Comma-separated file suffixes considered for directory scans. */
    protected static final String FILE_SUFFIXES_PARAM = "file_suffixes";

    private static final int DEFAULT_MAX_DEPTH = 10;

    private final String[] defaultFileSuffixes;

    /**
     * Creates a resolver.
     *
     * @param defaultFileSuffixes suffixes used when the {@code file_suffixes} parameter is unset
     */
    public JsonSourceResolver(final String[] defaultFileSuffixes) {
        this.defaultFileSuffixes = defaultFileSuffixes;
    }

    /**
     * Resolves the sources to crawl.
     *
     * @param paramMap the data store parameters
     * @return the sources, ordered oldest first within each directory
     */
    public List<JsonSource> resolve(final DataStoreParams paramMap) {
        final String files = paramMap.getAsString(FILES_PARAM);
        final String dirs = paramMap.getAsString(DIRS_PARAM);
        final String urls = paramMap.getAsString(URLS_PARAM);
        if (StringUtil.isBlank(files) && StringUtil.isBlank(dirs) && StringUtil.isBlank(urls)) {
            throw new DataStoreException(FILES_PARAM + ", " + DIRS_PARAM + " and " + URLS_PARAM + " are blank.");
        }

        final String[] suffixes = getFileSuffixes(paramMap);
        final Pattern includePattern = compile(paramMap.getAsString(INCLUDE_PATTERN_PARAM));
        final Pattern excludePattern = compile(paramMap.getAsString(EXCLUDE_PATTERN_PARAM));

        final List<JsonSource> sources = new ArrayList<>();
        if (StringUtil.isNotBlank(files)) {
            logger.info("{}={}", FILES_PARAM, files);
            for (final String path : splitValues(files)) {
                final File file = new File(path);
                if (!file.exists()) {
                    logger.warn("{} does not exist.", path);
                } else if (!file.isFile()) {
                    logger.warn("{} is not a file.", path);
                } else if (!hasDesiredSuffix(file.getName(), suffixes)) {
                    logger.warn("{} is skipped because its suffix is not one of {}.", path, Arrays.toString(suffixes));
                } else if (accepts(file.getAbsolutePath(), includePattern, excludePattern)) {
                    sources.add(new FileJsonSource(file));
                }
            }
        }

        if (StringUtil.isNotBlank(dirs)) {
            logger.info("{}={}", DIRS_PARAM, dirs);
            final boolean recursive = Boolean.parseBoolean(paramMap.getAsString(RECURSIVE_PARAM, "false"));
            final int maxDepth = getMaxDepth(paramMap);
            for (final String path : splitValues(dirs)) {
                final File dir = new File(path);
                if (dir.isDirectory()) {
                    final List<File> found = new ArrayList<>();
                    collect(dir, suffixes, includePattern, excludePattern, recursive, maxDepth, 0, found);
                    found.sort(Comparator.comparingLong(File::lastModified));
                    found.forEach(f -> sources.add(new FileJsonSource(f)));
                } else {
                    logger.warn("{} is not a directory.", path);
                }
            }
        }

        if (sources.isEmpty() && logger.isDebugEnabled()) {
            logger.debug("No sources for files={}, directories={}, urls={}", files, dirs, urls);
        }
        return sources;
    }

    /**
     * Collects matching files below a directory.
     *
     * @param dir the directory to scan
     * @param suffixes accepted file suffixes
     * @param includePattern names must match this, when non-null
     * @param excludePattern names must not match this, when non-null
     * @param recursive whether to descend into subdirectories
     * @param maxDepth how far below {@code dir} recursion may go
     * @param depth the current depth
     * @param out collected files
     */
    private void collect(final File dir, final String[] suffixes, final Pattern includePattern, final Pattern excludePattern,
            final boolean recursive, final int maxDepth, final int depth, final List<File> out) {
        final File[] entries = dir.listFiles();
        if (entries == null) {
            logger.warn("Failed to list {}.", dir.getAbsolutePath());
            return;
        }
        for (final File entry : entries) {
            if (entry.isDirectory()) {
                if (recursive && depth < maxDepth) {
                    collect(entry, suffixes, includePattern, excludePattern, recursive, maxDepth, depth + 1, out);
                }
            } else if (hasDesiredSuffix(entry.getName(), suffixes) && accepts(entry.getAbsolutePath(), includePattern, excludePattern)) {
                out.add(entry);
            }
        }
    }

    /**
     * Splits a comma-separated parameter value, dropping blank elements.
     *
     * @param value the value to split
     * @return trimmed, non-blank elements
     */
    protected String[] splitValues(final String value) {
        return Arrays.stream(value.split(",")).map(String::trim).filter(StringUtil::isNotBlank).toArray(String[]::new);
    }

    /**
     * Returns the accepted file suffixes.
     *
     * @param paramMap the data store parameters
     * @return suffixes from the parameter, or the defaults
     */
    protected String[] getFileSuffixes(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(FILE_SUFFIXES_PARAM);
        if (StringUtil.isBlank(value)) {
            return defaultFileSuffixes;
        }
        return splitValues(value);
    }

    private int getMaxDepth(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(MAX_DEPTH_PARAM);
        if (StringUtil.isBlank(value)) {
            return DEFAULT_MAX_DEPTH;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (final NumberFormatException e) {
            logger.warn("{} is not an int value: {}. Using {}.", MAX_DEPTH_PARAM, value, DEFAULT_MAX_DEPTH, e);
            return DEFAULT_MAX_DEPTH;
        }
    }

    private Pattern compile(final String value) {
        return StringUtil.isBlank(value) ? null : Pattern.compile(value.trim());
    }

    private boolean accepts(final String name, final Pattern includePattern, final Pattern excludePattern) {
        if (includePattern != null && !includePattern.matcher(name).matches()) {
            return false;
        }
        return excludePattern == null || !excludePattern.matcher(name).matches();
    }

    private boolean hasDesiredSuffix(final String filename, final String[] suffixes) {
        final String name = filename.toLowerCase(Locale.ROOT);
        for (final String suffix : suffixes) {
            if (name.endsWith(suffix.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }
}
