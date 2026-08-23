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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
     * <p>
     * Each file is returned at most once, however many of the configured roots reach it: a path
     * named in {@code files} that also sits in a scanned directory, or two directory roots where
     * one contains the other, would otherwise be read and indexed twice over.
     * </p>
     *
     * @param paramMap the data store parameters
     * @return the sources, ordered oldest first within each directory
     * @throws DataStoreException if no source parameter is set, if {@code urls} is set, or if a
     *             pattern parameter is not a valid regular expression
     */
    public List<JsonSource> resolve(final DataStoreParams paramMap) {
        final String files = paramMap.getAsString(FILES_PARAM);
        final String dirs = paramMap.getAsString(DIRS_PARAM);
        final String urls = paramMap.getAsString(URLS_PARAM);
        if (StringUtil.isBlank(files) && StringUtil.isBlank(dirs) && StringUtil.isBlank(urls)) {
            throw new DataStoreException(FILES_PARAM + ", " + DIRS_PARAM + " and " + URLS_PARAM + " are blank.");
        }
        rejectUnsupportedUrls(urls);

        final String[] suffixes = getFileSuffixes(paramMap);
        final Pattern includePattern = compilePattern(INCLUDE_PATTERN_PARAM, paramMap.getAsString(INCLUDE_PATTERN_PARAM));
        final Pattern excludePattern = compilePattern(EXCLUDE_PATTERN_PARAM, paramMap.getAsString(EXCLUDE_PATTERN_PARAM));

        // Both sets span the whole call rather than one root, which is what makes overlapping
        // roots read each file once. visitedDirs additionally stops a symlink cycle; see collect.
        final Set<String> visitedDirs = new HashSet<>();
        final Set<String> collectedFiles = new HashSet<>();

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
                } else if (accepts(file.getAbsolutePath(), includePattern, excludePattern) && collectedFiles.add(identityOf(file))) {
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
                if (!dir.exists()) {
                    logger.warn("{} does not exist.", path);
                } else if (!dir.isDirectory()) {
                    logger.warn("{} is not a directory.", path);
                } else {
                    if (recursive) {
                        final String canonicalRoot = canonicalPathOrNull(dir);
                        if (canonicalRoot == null || !visitedDirs.add(canonicalRoot)) {
                            // Either unresolvable, or already walked as a descendant of an
                            // earlier root - "/a" then "/a/sub" - so there is nothing left here.
                            continue;
                        }
                    }
                    final List<File> found = new ArrayList<>();
                    collect(dir, suffixes, includePattern, excludePattern, recursive, maxDepth, 0, visitedDirs, collectedFiles, found);
                    found.sort(Comparator.comparingLong(File::lastModified));
                    found.forEach(f -> sources.add(new FileJsonSource(f)));
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
     * @param visited canonical paths of directories already walked by this resolve call, used to
     *            stop symlink cycles and overlapping roots; unused when {@code recursive} is
     *            {@code false}
     * @param collected identities of files already collected by this resolve call, so that
     *            overlapping roots do not hand the same file to the crawl twice
     * @param out collected files
     */
    private void collect(final File dir, final String[] suffixes, final Pattern includePattern, final Pattern excludePattern,
            final boolean recursive, final int maxDepth, final int depth, final Set<String> visited, final Set<String> collected,
            final List<File> out) {
        final File[] entries = dir.listFiles();
        if (entries == null) {
            logger.warn("Failed to list {}.", dir.getAbsolutePath());
            return;
        }
        for (final File entry : entries) {
            if (entry.isDirectory()) {
                if (recursive && depth < maxDepth) {
                    // A symlink can point back at an ancestor directory, e.g. "a/loop -> a". Without
                    // cycle detection that recurses forever, bounded only by max_depth, and every
                    // level re-lists the same real directory, duplicating every file in it. We key
                    // on the canonical (symlink-resolved) path, seeded with the scan's own root
                    // directory, and skip anything already seen on this walk.
                    final String canonical = canonicalPathOrNull(entry);
                    if (canonical == null || !visited.add(canonical)) {
                        logger.warn("Skipping {} to avoid an unbounded or duplicate walk.", entry.getAbsolutePath());
                        continue;
                    }
                    collect(entry, suffixes, includePattern, excludePattern, recursive, maxDepth, depth + 1, visited, collected, out);
                }
            } else if (hasDesiredSuffix(entry.getName(), suffixes) && accepts(entry.getAbsolutePath(), includePattern, excludePattern)
                    && collected.add(identityOf(entry))) {
                out.add(entry);
            }
        }
    }

    /**
     * Returns the key that identifies a file for de-duplication.
     *
     * <p>
     * The canonical path is what makes two roots reaching one file compare equal even when they
     * reach it by different names - through a symlinked directory, or through {@code ..}. When it
     * cannot be resolved the absolute path is used instead: that is weaker, but the only cost of
     * missing a match here is reading a file twice, which is what this already did.
     * </p>
     *
     * @param file the file to identify
     * @return its canonical path, or its absolute path when that cannot be resolved
     */
    private String identityOf(final File file) {
        final String canonical = canonicalPathOrNull(file);
        return canonical != null ? canonical : file.getAbsolutePath();
    }

    /**
     * Resolves a file's canonical (symlink-free) path, for cycle detection.
     *
     * <p>
     * {@link File#getCanonicalPath()} can fail, for example on a symlink whose target cannot be
     * resolved. When that happens we cannot tell whether the entry is a cycle, so we choose to skip
     * it rather than risk descending into one: a missed directory is a minor gap, an unbounded or
     * duplicate walk is a real failure.
     * </p>
     *
     * @param file the file to resolve
     * @return the canonical path, or {@code null} if it could not be resolved
     */
    private String canonicalPathOrNull(final File file) {
        try {
            return file.getCanonicalPath();
        } catch (final IOException e) {
            logger.warn("Failed to resolve the canonical path of {}.", file.getAbsolutePath(), e);
            return null;
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

    /**
     * Refuses the {@code urls} parameter until remote sources are implemented.
     *
     * <p>
     * Remote sources are a later phase. Until then the parameter is refused rather than accepted
     * and ignored: ignoring it means no exception, no failure record and no documents, which is a
     * worse answer to a configuration this resolver cannot honour than saying so. Exposed
     * separately from {@link #resolve(DataStoreParams)} so that a caller can make the same check
     * early and attribute the failure to this parameter rather than to the data config.
     * </p>
     *
     * @param value the parameter value, may be {@code null} or blank
     * @throws DataStoreException if the value is not blank
     */
    protected static void rejectUnsupportedUrls(final String value) {
        if (StringUtil.isNotBlank(value)) {
            throw new DataStoreException(URLS_PARAM + " is not supported yet. Use " + FILES_PARAM + " or " + DIRS_PARAM + " instead.");
        }
    }

    /**
     * Compiles the value of a pattern parameter.
     *
     * <p>
     * A value that is not a valid regular expression is reported against the parameter that
     * carries it. Left alone, {@link Pattern#compile(String)} throws a
     * {@link PatternSyntaxException} that names only the expression, and callers have no way to
     * say which of the two pattern parameters produced it.
     * </p>
     *
     * @param paramName the parameter the value came from
     * @param value the value, may be {@code null} or blank
     * @return the compiled pattern, or {@code null} when the value is blank
     * @throws DataStoreException if the value is not a valid regular expression
     */
    protected static Pattern compilePattern(final String paramName, final String value) {
        if (StringUtil.isBlank(value)) {
            return null;
        }
        try {
            return Pattern.compile(value.trim());
        } catch (final PatternSyntaxException e) {
            throw new DataStoreException(paramName + " is not a valid regular expression: " + value, e);
        }
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
