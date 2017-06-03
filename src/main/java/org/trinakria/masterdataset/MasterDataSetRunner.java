package org.trinakria.masterdataset;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static org.trinakria.masterdataset.FileUtils.countFiles;
import static org.trinakria.masterdataset.FileUtils.sizeOf;
import static org.trinakria.masterdataset.FileUtils.writeFile;
import static org.trinakria.masterdataset.MasterDataSetSpec.FILENAME_TEMPLATE;
import static org.trinakria.masterdataset.MasterDataSetSpec.Mode.GENERATE;
import static org.trinakria.masterdataset.MasterDataSetSpec.ONE_MB;

/**
 * Created by Mario on 01/06/2017.
 */
public class MasterDataSetRunner {

    private void run(MasterDataSetSpec specification) {

        switch (specification.mode()) {
            case GENERATE: {
                generate(specification);
                break;
            }
            case UPDATE: {
                update(specification);
                break;
            }
            default: {
                throw new IllegalArgumentException(format("Unknown mode %s", specification.mode()));
            }
        }
    }

    private void generate(MasterDataSetSpec specification) {
        FileUtils.createDirIfNotExist(specification.inputFolder());
        long fileSizeMB = specification.fileSizeMB();
        specification.dataSets().forEach(dataSet -> {
            String dataSetName = dataSet.dataSetName();
            long dataSetSizeMB = dataSet.dataSetSizeMB();
            System.out.println(format("Creating data set %s of size %dMB", dataSetName, dataSetSizeMB));
            long noOfFiles = specification.noOfFilesInDataSet(dataSetSizeMB);
            long fileSizeByte = specification.fileSizeByte();
            long lastFileSizeByte = (dataSetSizeMB - (noOfFiles-1) * fileSizeMB) * ONE_MB;
            Path dataSetFolder = specification.dataSetFolder(dataSetName);
            FileUtils.createDirIfNotExist(dataSetFolder);
            long seed = dataSet.seed();
            LongStream.range(seed, noOfFiles + seed).forEach(idx -> {
                        Path filePath = Paths.get(dataSetFolder.toString(), format(FILENAME_TEMPLATE, idx));
                        long currentFileSizeByteLimit;
                        if ((noOfFiles + seed) - 1 == idx) {
                            currentFileSizeByteLimit = lastFileSizeByte;
                        } else {
                            currentFileSizeByteLimit = fileSizeByte;
                        }
                        FileUtils.writeFile(filePath, currentFileSizeByteLimit);
            });
        });


    }

    private void update(MasterDataSetSpec specification) {
        Validate.isTrue(Files.exists(specification.inputFolder()), "An existing input folder is mandatory " +
                "in update mode");
        specification.dataSets().forEach(dataSet -> {
            String dataSetName = dataSet.dataSetName();
            Path dataSetFolder = specification.dataSetFolder(dataSetName);
            if (Files.exists(dataSetFolder)) {
                System.out.println(format("Expanding data set %s of size %dMB", dataSetName, dataSet.dataSetSizeMB()));
                long count = countFiles(dataSetFolder);
                Path smallest = findSmallestFileInDateSet(dataSetFolder);
                Path reference = findReferenceFileSize(dataSetFolder, smallest);
                long smallestGrowSizeByte = sizeOf(reference) - sizeOf(smallest); //don't grow if diff less than 200KB?
                long remainingGrowSizeByte = 0L;
                long dataSetGrowSizeByte = dataSet.dataSetSizeMB() * ONE_MB;
                if (smallestGrowSizeByte >= dataSetGrowSizeByte) {
                    smallestGrowSizeByte = dataSetGrowSizeByte;
                } else {
                    remainingGrowSizeByte = dataSetGrowSizeByte - smallestGrowSizeByte;
                }

                Validate.isTrue(smallestGrowSizeByte + remainingGrowSizeByte == dataSetGrowSizeByte,
                        "Something went wrong in growing the dataset");

                writeFile(smallest, smallestGrowSizeByte, StandardOpenOption.APPEND);

                MasterDataSetSpec.DataSet newDataSet = MasterDataSetSpec.DataSet.of(dataSetName, remainingGrowSizeByte/ONE_MB, count);
                MasterDataSetSpec newSpecification = new MasterDataSetSpec(GENERATE,
                                                                           specification.inputFolder(),
                                                                           sizeOf(reference)/ONE_MB,
                                                                           Arrays.asList(newDataSet));
                generate(newSpecification);
            } else {
                System.out.println("Unknown data set folder " + dataSetFolder);
            }
        });

    }

    private Path findReferenceFileSize(Path dataSetFolder, Path smallest) {
        try {
            try (Stream<Path> stream = Files.walk(dataSetFolder, 1)) {
                Path reference = stream
                        .filter(path -> sizeOf(path) > sizeOf(smallest))
                        .findFirst()
                        .get();
                return reference;
            }
        } catch (IOException e) {
            throw new RuntimeException(format("Cannot find reference file in data set %s", dataSetFolder), e);
        }
    }

    private Path findSmallestFileInDateSet(Path dataSetDir) {
        try {
            try (Stream<Path> stream = Files.list(dataSetDir)) {
                Path smallest = stream
                        .min(comparingLong(FileUtils::sizeOf))
                        .get();
                return smallest;
            }
        } catch (IOException e) {
            throw new RuntimeException(format("Cannot find smallest file in data set %s", dataSetDir), e);
        }
    }


    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();
        MasterDataSetSpec specification = MasterDataSetSpec.fromArgs(args);
        System.out.println("Running master data set with specification: " + specification);
        new MasterDataSetRunner().run(specification);
        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Total execution time in millis: " + elapsedTime/1000000);
    }

}
