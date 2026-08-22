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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class JsonSourceResolverTest {

    private final JsonSourceResolver resolver = new JsonSourceResolver(new String[] { ".json", ".jsonl" });

    private List<String> names(final DataStoreParams params) {
        return resolver.resolve(params).stream().map(JsonSource::getName).map(n -> new File(n).getName()).collect(Collectors.toList());
    }

    @Test
    public void test_blankParameters_throws() {
        assertThrows(DataStoreException.class, () -> resolver.resolve(new DataStoreParams()));
    }

    @Test
    public void test_filesAndDirectories_areCombined() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path indir = dir.resolve("indir.json");
        Path standalone = null;
        try {
            Files.createFile(indir);
            standalone = Files.createTempFile("standalone", ".jsonl");

            final DataStoreParams params = new DataStoreParams();
            params.put("files", standalone.toString());
            params.put("directories", dir.toString());

            final List<String> names = names(params);
            assertEquals(2, names.size(), "both parameters contribute: " + names);
            assertTrue(names.contains("indir.json"), names.toString());
            assertTrue(names.contains(standalone.getFileName().toString()), names.toString());
        } finally {
            if (standalone != null) {
                Files.deleteIfExists(standalone);
            }
            Files.deleteIfExists(indir);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_directories_areNotRecursiveByDefault() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path top = dir.resolve("top.json");
        final Path sub = dir.resolve("sub");
        final Path nested = sub.resolve("nested.json");
        try {
            Files.createFile(top);
            Files.createDirectory(sub);
            Files.createFile(nested);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());

            assertEquals(List.of("top.json"), names(params));
        } finally {
            Files.deleteIfExists(nested);
            Files.deleteIfExists(sub);
            Files.deleteIfExists(top);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_directories_recursive() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path top = dir.resolve("top.json");
        final Path sub = dir.resolve("sub");
        final Path nested = sub.resolve("nested.json");
        try {
            Files.createFile(top);
            Files.createDirectory(sub);
            Files.createFile(nested);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("recursive", "true");

            final List<String> names = names(params);
            assertEquals(2, names.size(), names.toString());
            assertTrue(names.contains("nested.json"), names.toString());
        } finally {
            Files.deleteIfExists(nested);
            Files.deleteIfExists(sub);
            Files.deleteIfExists(top);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_maxDepth_limitsRecursion() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path a = dir.resolve("a");
        final Path b = a.resolve("b");
        final Path depth1 = a.resolve("depth1.json");
        final Path depth2 = b.resolve("depth2.json");
        try {
            Files.createDirectory(a);
            Files.createDirectory(b);
            Files.createFile(depth1);
            Files.createFile(depth2);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("recursive", "true");
            params.put("max_depth", "1");

            assertEquals(List.of("depth1.json"), names(params));
        } finally {
            Files.deleteIfExists(depth2);
            Files.deleteIfExists(b);
            Files.deleteIfExists(depth1);
            Files.deleteIfExists(a);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_includePattern() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path keep = dir.resolve("keep.json");
        final Path drop = dir.resolve("drop.json");
        try {
            Files.createFile(keep);
            Files.createFile(drop);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("include_pattern", ".*keep\\.json");

            assertEquals(List.of("keep.json"), names(params));
        } finally {
            Files.deleteIfExists(keep);
            Files.deleteIfExists(drop);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_excludePattern() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path keep = dir.resolve("keep.json");
        final Path drop = dir.resolve("drop.json");
        try {
            Files.createFile(keep);
            Files.createFile(drop);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("exclude_pattern", ".*drop\\.json");

            assertEquals(List.of("keep.json"), names(params));
        } finally {
            Files.deleteIfExists(keep);
            Files.deleteIfExists(drop);
            Files.deleteIfExists(dir);
        }
    }

    /**
     * A leading {@code .*} makes {@code find()} succeed at offset 0 too, so a pattern like
     * {@code .*keep\.json} cannot tell {@code matches()} from {@code find()} apart when the only
     * candidates are an exact name and an unrelated one. This test uses a name that CONTAINS the
     * pattern's target as a strict prefix but is not equal to it, so only full-string matching
     * excludes it.
     */
    @Test
    public void test_includePattern_usesFullMatchNotSubstring() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path keep = dir.resolve("keep.json");
        final Path keepKeep = dir.resolve("keep.json.json");
        try {
            Files.createFile(keep);
            Files.createFile(keepKeep);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("include_pattern", ".*keep\\.json");

            assertEquals(List.of("keep.json"), names(params));
        } finally {
            Files.deleteIfExists(keep);
            Files.deleteIfExists(keepKeep);
            Files.deleteIfExists(dir);
        }
    }

    /**
     * Same discriminator as {@link #test_includePattern_usesFullMatchNotSubstring()}, on the
     * exclude side: under {@code find()}, {@code drop.json.json} would also be excluded because it
     * contains "drop.json" as a substring; under {@code matches()} only the exact name is.
     */
    @Test
    public void test_excludePattern_usesFullMatchNotSubstring() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path drop = dir.resolve("drop.json");
        final Path dropDrop = dir.resolve("drop.json.json");
        try {
            Files.createFile(drop);
            Files.createFile(dropDrop);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("exclude_pattern", ".*drop\\.json");

            assertEquals(List.of("drop.json.json"), names(params));
        } finally {
            Files.deleteIfExists(drop);
            Files.deleteIfExists(dropDrop);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_fileSuffixes_parameterOverridesDefault() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path ndjson = dir.resolve("a.ndjson");
        final Path json = dir.resolve("b.json");
        try {
            Files.createFile(ndjson);
            Files.createFile(json);

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("file_suffixes", ".ndjson");

            assertEquals(List.of("a.ndjson"), names(params));
        } finally {
            Files.deleteIfExists(ndjson);
            Files.deleteIfExists(json);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_sortedByLastModified_beyondIntRange() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path olderPath = dir.resolve("older.json");
        final Path newerPath = dir.resolve("newer.json");
        try {
            final File older = Files.createFile(olderPath).toFile();
            final File newer = Files.createFile(newerPath).toFile();
            final long now = System.currentTimeMillis();
            assertTrue(older.setLastModified(now - 30L * 24 * 60 * 60 * 1000));
            assertTrue(newer.setLastModified(now));

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());

            assertEquals(List.of("older.json", "newer.json"), names(params));
        } finally {
            Files.deleteIfExists(olderPath);
            Files.deleteIfExists(newerPath);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_commaSeparatedPaths_areTrimmed() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path aPath = dir.resolve("a.json");
        final Path bPath = dir.resolve("b.jsonl");
        try {
            final Path a = Files.createFile(aPath);
            final Path b = Files.createFile(bPath);

            final DataStoreParams params = new DataStoreParams();
            params.put("files", a + " , " + b + " , ");

            assertEquals(2, names(params).size());
        } finally {
            Files.deleteIfExists(aPath);
            Files.deleteIfExists(bPath);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_nonExistentFile_isSkipped() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path okPath = dir.resolve("ok.json");
        try {
            final Path ok = Files.createFile(okPath);
            final DataStoreParams params = new DataStoreParams();
            params.put("files", ok + ",/nonexistent/path/missing.json");

            assertEquals(List.of("ok.json"), names(params));
        } finally {
            Files.deleteIfExists(okPath);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void test_nonDirectoryPath_isSkipped() throws IOException {
        final Path file = Files.createTempFile("notadir", ".json");
        try {
            final DataStoreParams params = new DataStoreParams();
            params.put("directories", file.toString());

            assertEquals(0, names(params).size());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * A symlink cycle ({@code a/loop -> a}) must not be walked forever, and the real file inside
     * {@code a} must be collected exactly once, not once per level the walk manages to descend
     * before {@code max_depth} cuts it off. Skipped when this filesystem cannot create symbolic
     * links (some CI agents and Windows without elevated privileges refuse it).
     */
    @Test
    public void test_directories_recursive_symlinkCycle_collectsFileOnce() throws IOException {
        final Path dir = Files.createTempDirectory("res");
        final Path a = dir.resolve("a");
        final Path target = a.resolve("target.json");
        final Path loop = a.resolve("loop");
        try {
            Files.createDirectory(a);
            Files.createFile(target);
            try {
                Files.createSymbolicLink(loop, a);
            } catch (final IOException | UnsupportedOperationException e) {
                Assumptions.assumeTrue(false, "symlinks are not supported on this filesystem: " + e);
            }

            final DataStoreParams params = new DataStoreParams();
            params.put("directories", dir.toString());
            params.put("recursive", "true");
            params.put("max_depth", "50");

            final List<String> names = names(params);
            final long targetCount = names.stream().filter("target.json"::equals).count();
            assertEquals(1, targetCount, names.toString());
        } finally {
            Files.deleteIfExists(loop);
            Files.deleteIfExists(target);
            Files.deleteIfExists(a);
            Files.deleteIfExists(dir);
        }
    }
}
