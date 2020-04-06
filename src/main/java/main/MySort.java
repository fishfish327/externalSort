package main;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class MySort {
    static int bufferSize = 1024 * 8;
    static int bufferNum = 100;
    static long chunkNum = 1000;
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

            final List<File> tmpfiles = new ArrayList<>();
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

            mergetmpFiles(tmpfiles, output_file, chunk_merge_counter_calculator(freemem, tmpfiles.size()));

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
    private static int chunk_merge_counter_calculator(long free_mem, int size) {

        long estimate_mem = size * bufferNum * bufferSize;
        if (estimate_mem > free_mem) {
            return chunk_merge_counter_calculator(free_mem, size / 2);
        } else {

            System.out.println("Merge counter is " + size);
            return size;
        }
    }

    //Calculate the max file size for the chunk
    private static long calculateBlockSize(File file) {
        long sizeoffile = file.length();
        //Assuming max 1000 chunk files will be generated
        long blocksize = sizeoffile / chunkNum;
        long freemem = Runtime.getRuntime().freeMemory();
        System.out.println("Free memory found on the system is " + freemem / 1024 + " KB.");

        if(blocksize >= freemem)
        {
            System.err.println("We may run out of memory.");
        }
        System.out.println("Block size for reading is " + blocksize / 1024 + " KB");
        return blocksize;
    }

    //merge the chunk files, merge count describes, how many files should be merged at once.
    private static void mergetmpFiles(List<File> tmpfiles, String outfile, int merge_count) {
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

        }else {
            int c = 0;
            List<File> newtmpFiles = new ArrayList<>();
            List<MyFileMerger> merger = new ArrayList<>();

            int t = 0;
            //spawn the merger thread for the batch of tmp files
            while (c != 1) {

                List<File> files_to_merge = new ArrayList<>();
                if (tmpfiles.size() > merge_count) {
                    files_to_merge = tmpfiles.subList(0, merge_count);
                    tmpfiles = tmpfiles.subList(merge_count, tmpfiles.size());
                } else if (tmpfiles.size() == 1) {
                    newtmpFiles.add(tmpfiles.get(0));
                    break;
                } else {
                    files_to_merge = tmpfiles;
                    c = 1;
                }

                try {
                    t++;
                    System.out.println("Merging stage " + t + ".");
                    File tmp_merge_file = File.createTempFile("FileChunkMerge" + t, "Thread.tmp");
                    tmp_merge_file.deleteOnExit();
                    MyFileMerger th = new MyFileMerger(files_to_merge, tmp_merge_file);
                    merger.add(th);
                    newtmpFiles.add(tmp_merge_file);
                    th.start();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


}


