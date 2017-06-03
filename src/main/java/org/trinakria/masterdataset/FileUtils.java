package org.trinakria.masterdataset;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Created by Mario on 03/06/2017.
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
