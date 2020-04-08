package common;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class MyFilePartitioner {
    private static List<FileSlice> partition(File input, int sliceSize) throws IOException {
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

    public static List<File> partitionAndSort(List<File> tmpFiles, File input, int sliceSize, int numOfThread, int bufferSize) {

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numOfThread);

        try {
            List<FileSlice> slices = partition(input, sliceSize);
            List<Future<File>> futures = new ArrayList<>(slices.size());
            for (final FileSlice slice : slices) {
                futures.add(executor.submit(new Callable<File>() {
                    @Override
                    public File call() throws Exception {
                        return writeSlice(slice, input, bufferSize);
                    }
                }));
            }

            // get File
            for (int i = 0; i < futures.size(); i++) {
                File result = futures.get(i).get();
                tmpFiles.add(result);
            }
            executor.shutdown();
            System.out.println("Generated " + tmpFiles.size() + " batch files.");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return tmpFiles;
    }

    private static File writeSlice(FileSlice slice, File input, int bufferSize) throws IOException {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new RandomAccessFileInputStream(input,
                    slice.begin, slice.end)), bufferSize);

            List<String> lines = new ArrayList<>();
            String line = null;
            while (true) {
                line = reader.readLine();
                if (line == null) {
                    break;
                }
                lines.add(line);
            }
            Collections.sort(lines);
            File tmpFile = File.createTempFile("FileChunk", "thread");
            tmpFile.deleteOnExit();
            writer = new BufferedWriter(new FileWriter(tmpFile), bufferSize);
            for (String l : lines) {
                writer.write(l);
                writer.newLine();
            }
            writer.flush();
            return tmpFile;
        } finally {
            IOUtil.closeQuietly(reader);
            IOUtil.closeQuietly(writer);
        }

    }

}
