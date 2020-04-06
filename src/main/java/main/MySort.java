package main;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class MySort {
    // parameter numOfThread
    public static void main(String args[]) {
        //check for input
        if (args.length <  3) {
            System.out.println("Usage [input file] [output file] [number of thread]");
            return;
        }

        try {
            String input_file = args[0];
            String output_file = args[1];

            int r_n = Integer.parseInt(args[2]);
            long startTime = System.nanoTime();


            File input = new File(input_file);
            long bs = calculateBlockSize(input);


            final BufferedReader br = new BufferedReader(new FileReader(input));

            final LinkedList<File> tmpfiles = new LinkedList<>();
            MyFileReader threads[] = new MyFileReader[r_n];

            //read the input file by spawning 4 threads
            for (int i = 0; i < r_n; i++) {
                threads[i] = new MyFileReader(br, bs, tmpfiles, i);
                threads[i].run();

            }
            for (int i = 0; i < r_n; i++) {
                threads[i].join();

            }

            System.out.println("Generated " + tmpfiles.size() + " batch files.");
            long freemem = Runtime.getRuntime().freeMemory();
            System.out.println("Free memory during merge is " + freemem / 1024 + " KB.");
            int mergeCount = chunk_merge_counter_calculator(freemem, tmpfiles.size(), r_n);

            // start thread to merge file
            mergeFileWithFixThread(tmpfiles, output_file, r_n, mergeCount);
            //mergetmpFiles(tmpfiles, output_file, chunk_merge_counter_calculator(freemem, tmpfiles.size()));

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            System.out.println("Time required for MySort is " + totalTime / 1000000000.0 + " seconds. \n");

        } catch (NumberFormatException e) {
            System.out.print("Provided input (3rd or 4th or 5th ) in incorrect");
        } catch (FileNotFoundException e) {
            System.out.print("Provided input (1st or 2nd) in incorrect");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //calculate how many chunk files should be merged at once
    private static int chunk_merge_counter_calculator(long free_mem, int size, int r_n) {

        if (size * 1024 * 8 * r_n * 100> free_mem) {
            return chunk_merge_counter_calculator(free_mem, size / 2, r_n);
        } else {
            System.out.println("Merge counter is " + size);
            return size;
        }
    }

    //Calculate the max file size for the chunk
    private static long calculateBlockSize(File file) {
        long sizeoffile = file.length();
        //Assuming max 1000 chunk files will be generated
        long blocksize = sizeoffile / 1000;
        long freemem = Runtime.getRuntime().freeMemory();
        System.out.println("Free memory found on the system is " + freemem / 1024 + " KB.");


       /* if( blocksize < freemem/4)
        {
            blocksize = freemem/2;
        }
        else if(blocksize >= freemem)
        {
            System.err.println("We may run out of memory.");
        } */
        System.out.println("Block size for reading is " + blocksize / 1024 + " KB");
        return blocksize;
    }

    //merge the chunk files, merge count describes, how many files should be merged at once.
    private static void mergetmpFiles(LinkedList<File> tmpfiles, String outfile, int merge_count) {
        if (tmpfiles.size() <= merge_count) {
            try {
                File out_file = new File(outfile);
                MyFileMerger t = new MyFileMerger(tmpfiles, out_file);
                t.start();
                t.join();
                System.out.println("Sorting completed! Wrote output in " + outfile);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private static void mergeFileWithFixThread(LinkedList<File> tmpfiles, String outfile, int r_n, int merge_count) throws InterruptedException{
        LinkedList<File> mergeResult = new LinkedList<>();
        FileMergeThread[] mergeThreads = new FileMergeThread[merge_count];
        if(tmpfiles.size() < r_n){
            // use single thread to merge
            mergetmpFiles(tmpfiles, outfile,  merge_count);
        } else {
            for(int i = 0; i < r_n; i++){
                mergeThreads[i] = new FileMergeThread(tmpfiles, mergeResult, merge_count);
                mergeThreads[i].start();
            }
            for(int i = 0; i < r_n; i++){
                mergeThreads[i].join();
            }
            mergeFileWithFixThread(mergeResult, outfile, r_n, merge_count);
        }
    }


}


