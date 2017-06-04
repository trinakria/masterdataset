package org.trinakria.masterdataset;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Comparator.comparingLong;
import static org.trinakria.masterdataset.FileUtils.*;
import static org.trinakria.masterdataset.MasterDataSetSpec.FILENAME_TEMPLATE;
import static org.trinakria.masterdataset.MasterDataSetSpec.Mode.GENERATE;
import static org.trinakria.masterdataset.MasterDataSetSpec.ONE_MB;

/**
 * Main class of master data set example. It supports generation, updates and backup of a master data set described
 * by {@link MasterDataSetSpec}
 *
 * @author Mario Giurlanda
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
            case BACKUP: {
                backup(specification);
                break;
            }
            default: {
                throw new IllegalArgumentException(format("Unknown mode %s", specification.mode()));
            }
        }
    }

    /**
     * Generates a master data set from the input specification.
     *
     * All files in each data set will have roughly the same size expect the last one that can be smaller to meet
     * the data set max size constraint.
     *
     * @param specification master data set specification
     */
    private void generate(MasterDataSetSpec specification) {
        FileUtils.createDirIfNotExist(specification.inputFolder());
        long fileSizeMB = specification.fileSizeMB();
        specification.dataSets().forEach(dataSet -> {
            String dataSetName = dataSet.dataSetName();
            long dataSetSizeMB = dataSet.dataSetSizeMB();
            System.out.format("Creating data set %s of size %dMB%n", dataSetName, dataSetSizeMB);
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

    /**
     * Updates a master data set from the input specification.
     *
     * Each data set will be enlarged with the following strategy:
     * <ul>
     *     <li>Find smallest file in the data set</li>
     *     <li>Find the max size of a file in the data set, which is the size of the any file that is not the smallest
     *         (assuming they all have same size) </li>
     *     <li>Grow smallest file to smallest between the max file size or the data set enlarging size
     *         (defined by {@link MasterDataSetSpec.DataSet#dataSetSizeMB})</li>
     *     <li>Enlarge the data set of the remaining size by delegating to {@link #generate(MasterDataSetSpec)}</li>
     * </ul>
     *
     * @param specification master data set specification
     */
    private void update(MasterDataSetSpec specification) {
        Validate.isTrue(Files.exists(specification.inputFolder()), "An existing input folder is mandatory " +
                "in update mode");
        specification.dataSets().forEach(dataSet -> {
            String dataSetName = dataSet.dataSetName();
            Path dataSetFolder = specification.dataSetFolder(dataSetName);
            if (Files.exists(dataSetFolder)) {
                System.out.format("Expanding data set %s of size %dMB", dataSetName, dataSet.dataSetSizeMB());
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
                System.out.format("Unknown data set folder %s%n", dataSetFolder);
            }
        });

    }

    /**
     * Backups a master data set from the input specification by deep copying the content of
     * {@link MasterDataSetSpec#inputFolder} to {@link MasterDataSetSpec#backupFolder}
     *
     * If {@link MasterDataSetSpec#backupFolder} already exists, it is renamed by appending current timestamp.
     *
     * @param specification master data set specification
     */
    private void backup(MasterDataSetSpec specification) {

        Path inputFolder = specification.inputFolder();
        Path backupFolder = specification.backupFolder().get();

        Validate.isTrue(Files.exists(inputFolder), "An existing input folder is mandatory " +
                "in backup mode");

        Validate.notNull(backupFolder, "A backup folder is mandatory in backup mode");

        createDirIfNotExist(backupFolder);

        Path backupPath = Paths.get(backupFolder.toString(), inputFolder.toString());

        if (Files.exists(backupPath)) {
            Path oldBackupPath = Paths.get(backupFolder.toString(), format("%s%d", inputFolder.toString(), System.currentTimeMillis()));
            System.out.format("Moving existing backup path to %s%n", oldBackupPath);
            try {
                Files.move(backupPath, oldBackupPath);
            } catch (IOException e) {
                throw new RuntimeException(format("Cannot backup %s to %s", inputFolder, backupFolder), e);
            }
        }

        FileUtils.copyFileTree(inputFolder, backupPath, false);
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
        System.out.format("Running master data set with specification: %s%n", specification);
        new MasterDataSetRunner().run(specification);
        long elapsedTime = System.nanoTime() - startTime;
        System.out.format("Total execution time in millis: %d%n", elapsedTime/1000000);
    }

}
