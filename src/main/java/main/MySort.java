package main;


import common.MyFilePartitioner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


public class MySort {
    static int inputBufferSize = 1024 * 8;
    static int outputBufferSize = 1024 * 8;
    static int chunkNum = 1024;
    static int numOfThread = 4;

    public static int chunkMergeCounterCalculator(long free_mem, int size) {

        long estimate_mem = size * inputBufferSize + outputBufferSize;
        if (estimate_mem > free_mem) {
            return chunkMergeCounterCalculator(free_mem, size / 2);
        } else {

            System.out.println("Merge counter is " + size);
            return size;
        }
    }

    //Calculate the max file size for the chunk
    public static long calculateBlockSize(File file) {
        long sizeOfFile = file.length();
        //Assuming max 1000 chunk files will be generated
        long blockSize = sizeOfFile / chunkNum;
        long freemem = Runtime.getRuntime().freeMemory();
        System.out.println("Free memory found on the system is " + freemem / 1024 + " KB.");

        if (blockSize >= freemem) {
            System.err.println("We may run out of memory.");
        }
        System.out.println("Block size for reading is " + blockSize / 1024 + " KB");
        return blockSize;
    }

    // n : num of the threads
    public static List<TmpFileBuffer> loadBuffer(List<File> tmpFileList, int n) throws InterruptedException {
        MyBufferLoader[] loaders = new MyBufferLoader[n];
        Queue<File> tmpFileQueue = new LinkedList<>();
        for (File f : tmpFileList) {
            tmpFileQueue.offer(f);
        }
        long startTime = System.nanoTime();
        List<TmpFileBuffer> bufferList = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            loaders[i] = new MyBufferLoader(tmpFileQueue, bufferList, inputBufferSize);
            loaders[i].start();
        }

        for (int i = 0; i < n; i++) {
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


    public static void main(String args[]) {
        //check for input
        if (args.length < 3) {
            System.out.println("Usage [input file] [output file] [number of thread] [sort mode]");
            return;
        }

        try {
            String input_file = args[0];
            String output_file = args[1];

            numOfThread= Integer.parseInt(args[2]);
            String mode = args[3];

            long startTime = System.nanoTime();

            /*
              Calculate block size
            */
            File input = new File(input_file);
            long bs = calculateBlockSize(input);
            long dataRead = 0L, dataWrite = 0L;
            long ioThroughput;
             /*
              Select mode to sort
            */

            if(mode.equals("internal")){
                File output = new File(output_file);
                MyFilePartitioner.InternalSort(input, output, (int)bs, numOfThread, inputBufferSize);
                // in MB
                dataRead = input.length() / 1000 / 1000;
                dataWrite = input.length() / 1000 / 1000;

            } else if(mode.equals("external")){
                final List<File> tmpfiles = new ArrayList<>();

                MyFilePartitioner.partitionAndSort(tmpfiles, input,(int) bs, numOfThread, inputBufferSize);

                long firstSortTime = (System.nanoTime() - startTime) / 1000000000;
                System.out.println("Time on first sort is: " + firstSortTime);
                long freemem = Runtime.getRuntime().freeMemory();
                System.out.println("Free memory during merge is " + freemem / 1024 + " KB.");

                mergetmpFiles(tmpfiles, output_file, chunkMergeCounterCalculator(freemem, tmpfiles.size()));
                dataRead = input.length() * 2 / 1000 / 1000;
                dataWrite = input.length() * 2 / 1000 / 1000;

            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            // ioThroughput in MB
            ioThroughput = (dataRead + dataWrite) / (long)(totalTime / 1000000000.0);


            System.out.println("Time required for MySort is " + totalTime / 1000000000.0 + " seconds. \n");
            System.out.println("Total Data Read is " + dataRead + " MB. \n");
            System.out.println("Total Data Write is " + dataWrite + " MB. \n");
            System.out.println("IO Throughput is " + ioThroughput + " MB/second. \n");


        } catch (NumberFormatException e) {
            System.out.print("Provided input (3rd or 4th or 5th ) in incorrect");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}


