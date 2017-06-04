package org.trinakria.masterdataset;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Contains utilities related to file operations.
 *
 * @author Mario Giurlanda
 */
public class FileUtils {

    static Path writeFile(Path filePath, long fileSizeByteLimit, OpenOption... openOptions) {
        long bytesOutput = 0;
        try {
            try (BufferedWriter writer = Files.newBufferedWriter(filePath, Charset.defaultCharset(), openOptions)) {
                while(bytesOutput <= fileSizeByteLimit)  {
                    String line = RandomStringUtils.randomAlphanumeric(100, 150);
                    writer.write(line);
                    writer.newLine();
                    bytesOutput += line.length();
                }
                return filePath;
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create file " + filePath, e);
        }
    }

    static void createDirIfNotExist(Path path) {
        if (Files.notExists(path)) {
            try {
                Files.createDirectory(path);
            } catch (IOException e) {
                throw new RuntimeException("Cannot create directory " + path, e);
            }
        }
    }

    static long sizeOf(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new RuntimeException("Cannot determine file size of " + path, e);
        }
    }

    static long countFiles(Path path) {
        try {
            try (Stream<Path> stream = Files.walk(path)) {
                return stream.parallel()
                        .filter(p -> !p.toFile().isDirectory())
                        .count();
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot count files in directory " + path, e);
        }
    }

    /**
     * Copy source file to target location. The {@code preserve}
     * parameter determines if file attributes should be copied/preserved.
     */
    static void copyFile(Path source, Path target, boolean preserve) {
        CopyOption[] options = (preserve) ?
                new CopyOption[] { COPY_ATTRIBUTES, REPLACE_EXISTING } :
                new CopyOption[] { REPLACE_EXISTING };
        if (Files.notExists(target)) {
            try {
                Files.copy(source, target, options);
            } catch (IOException e) {
                throw new RuntimeException(format("Unable to copy: %s to %s", source, target), e);
            }
        }
    }


    /**
     * Copy source file tree to target location. The {@code preserve}
     * parameter determines if file attributes should be copied/preserved.
     */
    static void copyFileTree(Path source, Path target, boolean preserve) {
        EnumSet<FileVisitOption> opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        TreeCopier tc = new TreeCopier(source, target, preserve);
        try {
            Files.walkFileTree(source, opts, Integer.MAX_VALUE, tc);
        } catch (IOException e) {
            throw new RuntimeException(format("Unable to copy: %s to %s", source, target), e);
        }
    }

    private static class TreeCopier implements FileVisitor<Path> {
        private final Path source;
        private final Path target;
        private final boolean preserve;

        TreeCopier(Path source, Path target, boolean preserve) {
            this.source = source;
            this.target = target;
            this.preserve = preserve;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            // before visiting entries in a directory we copy the directory
            // (okay if directory already exists).
            CopyOption[] options = (preserve) ?
                    new CopyOption[] { COPY_ATTRIBUTES } : new CopyOption[0];

            Path newdir = target.resolve(source.relativize(dir));
            try {
                Files.copy(dir, newdir, options);
            } catch (FileAlreadyExistsException x) {
                // ignore
            } catch (IOException x) {
                System.err.format("Unable to create: %s: %s%n", newdir, x);
                return SKIP_SUBTREE;
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            copyFile(file, target.resolve(source.relativize(file)), preserve);
            return CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            // fix up modification time of directory when done
            if (exc == null && preserve) {
                Path newdir = target.resolve(source.relativize(dir));
                try {
                    FileTime time = Files.getLastModifiedTime(dir);
                    Files.setLastModifiedTime(newdir, time);
                } catch (IOException x) {
                    System.err.format("Unable to copy all attributes to: %s: %s%n", newdir, x);
                }
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            if (exc instanceof FileSystemLoopException) {
                System.err.println("cycle detected: " + file);
            } else {
                System.err.format("Unable to copy: %s: %s%n", file, exc);
            }
            return CONTINUE;
        }
    }

    static void deepDelete(Path path) throws IOException {
        if (Files.notExists(path)) {
            return;
        }
        Files.walk(path, FileVisitOption.FOLLOW_LINKS)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .peek(System.out::println)
                .forEach(File::delete);
    }
}
