package org.trinakria.masterdataset;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * Created by Mario on 02/06/2017.
 */
public class MasterDataSetSpec {

    public enum Mode {
        GENERATE, UPDATE
    }

    public static final int ONE_MB = 1024 * 1024;
    private static final String DELIMITER = ",";
    private static final int PARTITION_SIZE = 2;
    static final String FILENAME_TEMPLATE = "file%d.txt";

    private final Path inputFolder;
    private final long fileSize;
    private final List<DataSet> dataSets;
    private final Mode mode;

    public MasterDataSetSpec(Mode mode, Path inputFolder, long fileSize, List<DataSet> dataSets) {
        this.inputFolder = inputFolder;
        this.fileSize = fileSize;
        this.dataSets = dataSets;
        this.mode = mode;
    }

    public static MasterDataSetSpec fromArgs(String[] args) {
        Validate.notEmpty(args, "Following args, separated by white space, are expected: \n" +
                "mode (GENERATE or UPDATE) \n" +
                "input folder \n" +
                "file size (mandatory in case of Generate mode)\n" +
                "data set structure");
        Mode mode = Mode.valueOf(args[0].toUpperCase());
        Path inputFolderArg = Paths.get(args[1]);
        long fileSize = 0;
        String structureStr = StringUtils.EMPTY;
        if (mode == Mode.GENERATE) {
            Validate.isTrue(args.length == 4, "4 args are expected: mode, input folder, file size, data set structure");
            fileSize = toLong(args[2]);
            Validate.isTrue(fileSize >= 0, "File size must be a positive number");
            structureStr = args[3];
        } else if (mode == Mode.UPDATE) {
            Validate.isTrue(args.length == 3, "3 args are expected: mode, input folder, file size, data set structure");
            structureStr = args[2];
        }

        List<DataSet> structure = new ArrayList<>();
        Iterable<String> split = Splitter.on(DELIMITER).split(structureStr);
        for (List<String> partition : Iterables.partition(split, PARTITION_SIZE)) {
            Validate.isTrue(partition.size() == PARTITION_SIZE, "Unmatched data set structure. Expected <name1>,<size1>,<name2>,<size2>");
            structure.add(DataSet.of(partition.get(0), toLong(partition.get(1))));
        }
        return new MasterDataSetSpec(mode, inputFolderArg, fileSize, structure);
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
                '}';
    }
}
