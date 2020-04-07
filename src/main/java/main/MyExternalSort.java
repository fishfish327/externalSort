package main;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class MyExternalSort {
    //calculate how many chunk files should be merged at once
    static long inputBufferSize = 1024 * 1024;
    static long outputBufferSize = 1024 * 1024 * 4;
    static long chunkNum = 1024;
    static int numOfThread;
    public static int chunk_merge_counter_calculator(long free_mem, int size) {

        long estimate_mem = size * inputBufferSize + outputBufferSize;
        if (estimate_mem > free_mem) {
            return chunk_merge_counter_calculator(free_mem, size / 2);
        } else {

            System.out.println("Merge counter is " + size);
            return size;
        }
    }

    //Calculate the max file size for the chunk
    public static long calculateBlockSize(File file) {
        long sizeoffile = file.length();
        //Assuming max 1000 chunk files will be generated
        long blocksize = sizeoffile / chunkNum;
        long freemem = Runtime.getRuntime().freeMemory();
        System.out.println("Free memory found on the system is " + freemem / 1024 + " KB.");

        if (blocksize >= freemem) {
            System.err.println("We may run out of memory.");
        }
        System.out.println("Block size for reading is " + blocksize / 1024 + " KB");
        return blocksize;
    }
    public static List<TmpFileBuffer> loadBuffer(List<File> tmpFileList, int r_n) throws InterruptedException {
        MyBufferLoader[] loaders = new MyBufferLoader[r_n];
        Queue<File> tmpFileQueue = new LinkedList<>();
        for(File f : tmpFileList){
            tmpFileQueue.offer(f);
        }
        long startTime = System.nanoTime();
        List<TmpFileBuffer> bufferList = new ArrayList<>();
        for(int i = 0; i < r_n; i++){
            loaders[i] = new MyBufferLoader(tmpFileQueue, bufferList, inputBufferSize);
            loaders[i].start();
        }

        for(int i = 0; i < r_n; i++){
            loaders[i].join();
        }
        long endTime = System.nanoTime();
        long timeCost = (endTime - startTime);
        System.out.println("load data time cost is : " + timeCost);
        return bufferList;
    }
    //merge the chunk files, merge count describes, how many files should be merged at once.
    public static void mergetmpFiles(List<File> tmpfiles, String outfile, int mergeCount) {
        if (tmpfiles.size() <= mergeCount) {
            try {
                File out_file = new File(outfile);

                // load data into buffer
                List<TmpFileBuffer> bufferList = loadBuffer(tmpfiles, numOfThread);
                MyFileMerger t = new MyFileMerger(bufferList, out_file);
                t.start();
                t.join();
                System.out.println("Sorting completed! Wrote output in " + outfile);
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {

            int c = 0;
            List<File> newTmpFiles = new ArrayList<>();
            List<MyFileMerger> mergerList = new ArrayList<>();

            int threadNumber = 0;
            //spawn the merger thread for the batch of tmp files
            while (c != 1) {

                List<File> filesToMerge = new ArrayList<>();
                if (tmpfiles.size() > mergeCount) {
                    filesToMerge = tmpfiles.subList(0, mergeCount);
                    tmpfiles = tmpfiles.subList(mergeCount, tmpfiles.size());
                } else if (tmpfiles.size() == 1) {
                    newTmpFiles.add(tmpfiles.get(0));
                    break;
                } else {
                    filesToMerge = tmpfiles;
                    c = 1;
                }

                try {
                    threadNumber++;
                    System.out.println("Merging stage " + threadNumber + ".");
                    File tmpMergeFile = File.createTempFile("FileChunkMerge" + threadNumber, "Thread.tmp");
                    tmpMergeFile.deleteOnExit();
                    List<TmpFileBuffer> bufferList = loadBuffer(filesToMerge, numOfThread);
                    MyFileMerger th = new MyFileMerger(bufferList, tmpMergeFile);
                    mergerList.add(th);
                    newTmpFiles.add(tmpMergeFile);
                    th.start();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            c = 0;
            for (MyFileMerger m : mergerList) {
                try {
                    m.join();
                    c++;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            mergetmpFiles(newTmpFiles, outfile, mergeCount);

        }
    }

}
