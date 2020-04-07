package common;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class MyFilePartitioner {
    public static List<FileSlice> partition(File input, int sliceSize) throws IOException {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(input, "r");

            List<FileSlice> slices = new ArrayList<FileSlice>();


            long bodyBegin = 0L;
            long bodyEnd = input.length();

            for (raf.seek(bodyBegin); bodyBegin < bodyEnd; bodyBegin = raf.getFilePointer()) {
                raf.skipBytes(sliceSize);
                IOUtil.skipNextLine(raf);
                slices.add(new FileSlice(bodyBegin, Math.min(raf.getFilePointer(),
                        bodyEnd)));
            }

            return slices;

        } finally {
            IOUtil.closeQuietly(raf);
        }
    }
}
