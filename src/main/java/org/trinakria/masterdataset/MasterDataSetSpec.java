package org.trinakria.masterdataset;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static org.trinakria.masterdataset.MasterDataSetSpec.Mode.BACKUP;
import static org.trinakria.masterdataset.MasterDataSetSpec.Mode.GENERATE;
import static org.trinakria.masterdataset.MasterDataSetSpec.Mode.UPDATE;

/**
 * A specification to be used to handle a master data set.
 *
 * @author Mario Giurlanda
 */
public class MasterDataSetSpec {

    /**
     * Mode of operation on the data set.
     */
    public enum Mode {
        GENERATE, UPDATE, BACKUP
    }

    public static final int ONE_MB = 1024 * 1024;
    private static final String DELIMITER = ",";
    private static final int PARTITION_SIZE = 2;
    static final String FILENAME_TEMPLATE = "file%d.txt";

    /**
     * Master data set input folder/
     */
    private final Path inputFolder;
    /**
     * Max size of a file within a data set
     */
    private final long fileSize;
    /**
     * Lists of data set that are part of the master data set
     */
    private final List<DataSet> dataSets;
    /**
     * Mode of operation on the data set
     */
    private final Mode mode;
    /**
     * Optional back up folder to be used in case of {@link Mode#BACKUP}
     */
    private final Optional<Path> backupFolder;

    public MasterDataSetSpec(Mode mode, Path inputFolder, long fileSize, List<DataSet> dataSets) {
        this(mode, inputFolder, fileSize, dataSets, Optional.empty());
    }

    public MasterDataSetSpec(Mode mode, Path inputFolder, long fileSize, List<DataSet> dataSets, Optional<Path> backupFolder) {
        this.inputFolder = inputFolder;
        this.fileSize = fileSize;
        this.dataSets = dataSets;
        this.mode = mode;
        this.backupFolder = backupFolder;
    }

    /**
     * Factory method that creates a specification from an array of args.
     * @param args arguments
     * @return a {@MasterDataSetSpec}
     */
    public static MasterDataSetSpec fromArgs(String[] args) {
        Validate.notEmpty(args, "Usage: \n" +
                "GENERATE input_folder file_size <name1,size1>,<name2,size2> \n" +
                "UPDATE input_folder <name1,size1>,<name2,size2> \n" +
                "BACKUP input_folder backup_folder");

        Mode mode = Mode.valueOf(args[0].toUpperCase());
        Path inputFolderArg = Paths.get(args[1]);
        long fileSize = 0;
        List<DataSet> structure = Collections.EMPTY_LIST;
        Optional<Path> backupFolder = Optional.empty();

        switch (mode) {
            case GENERATE: {
                Validate.isTrue(args.length == 4, "4 args are expected: GENERATE, input_folder, file_size <name1,size1>,<name2,size2>");
                fileSize = toLong(args[2]);
                Validate.isTrue(fileSize >= 0, "file_size must be a positive number");
                structure = parseDataSetStructure( args[3]);
                break;
            }
            case UPDATE: {
                Validate.isTrue(args.length == 3, "3 args are expected: UPDATE, input_folder, <name1,size1>,<name2,size2>");
                structure = parseDataSetStructure(args[2]);
                break;
            }
            case BACKUP: {
                Validate.isTrue(args.length == 3, "3 args are expected: BACKUP, input_folder, backup_folder");
                backupFolder = Optional.of(Paths.get(args[2]));
                break;
            }
            default: {
                throw new IllegalArgumentException(format("Unknown mode %s", mode));
            }
        }

        return new MasterDataSetSpec(mode, inputFolderArg, fileSize, structure, backupFolder);
    }

    private static List<DataSet> parseDataSetStructure(String structureStr) {
        List<DataSet> structure = new ArrayList<>();
        Iterable<String> split = Splitter.on(DELIMITER).split(structureStr);
        for (List<String> partition : Iterables.partition(split, PARTITION_SIZE)) {
            Validate.isTrue(partition.size() == PARTITION_SIZE, "Unmatched data set structure. Expected <name1>,<size1>,<name2>,<size2>");
            structure.add(DataSet.of(partition.get(0), toLong(partition.get(1))));
        }
        return structure;
    }

    public Path inputFolder() {
        return inputFolder;
    }

    public long fileSizeMB() {
        return fileSize;
    }

    public long fileSizeByte() {
        return fileSizeMB() * ONE_MB;
    }

    public List<DataSet> dataSets() {
        return dataSets;
    }

    public Path dataSetFolder(String dataSetName) {
        return Paths.get(inputFolder().toString(), dataSetName);
    }

    public long noOfFilesInDataSet(long dataSetSizeMB) {
        return roundUp(dataSetSizeMB, fileSizeMB());
    }

    public Mode mode() {
        return mode;
    }

    public Optional<Path> backupFolder() {
        return backupFolder;
    }

    public static final class DataSet {
        private final String dataSetName;
        private final long dataSetSizeMB;
        private final long seed;

        private DataSet(String dataSetName, long dataSetSizeMB) {
            this(dataSetName, dataSetSizeMB, 0);
        }

        private DataSet(String dataSetName, long dataSetSizeMB, long seed) {
            this.dataSetName = dataSetName;
            this.dataSetSizeMB = dataSetSizeMB;
            this.seed = seed;
        }

        public static DataSet of(String dataSetName, long dataSetSizeMB) {
            return new DataSet(dataSetName, dataSetSizeMB);
        }

        public static DataSet of(String dataSetName, long dataSetSizeMB, long seed) {
            return new DataSet(dataSetName, dataSetSizeMB, seed);
        }

        public String dataSetName() {
            return dataSetName;
        }

        public long dataSetSizeMB() {
            return dataSetSizeMB;
        }

        public long seed() {
            return seed;
        }

        @Override
        public String toString() {
            return "DataSet{" +
                    "dataSetName='" + dataSetName + '\'' +
                    ", dataSetSizeMB=" + dataSetSizeMB +
                    ", seed=" + seed +
                    '}';
        }
    }

    //utilities

    private static long toLong(String str) {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("% is not a number", str));
        }
    }


    private static long roundUp(long num, long divisor) {
        return (num + divisor - 1) / divisor;
    }

    @Override
    public String toString() {
        return "MasterDataSetSpec{" +
                "mode=" + mode +
                ", inputFolder=" + inputFolder +
                ", fileSize=" + fileSize +
                ", dataSets=" + dataSets +
                ", backupFolder=" + backupFolder +
                '}';
    }
}
