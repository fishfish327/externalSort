package main;


import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


public class MySort {
    static long inputBufferSize = 1024 * 1024;
    static long outputBufferSize = 1024 * 1024 * 4;
    static long chunkNum = 1024;
    static int numOfThread;

    public static void partition(File input, List<File> tmpfiles, int r_n, long bs) throws FileNotFoundException{
        MyFileReader threads[] = new MyFileReader[r_n];
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(input), (int)inputBufferSize);
            //read the input file by spawning 4 threads
            for (int i = 0; i < r_n; i++) {

                threads[i] = new MyFileReader(bufferedReader, bs, tmpfiles, i);
                threads[i].run();

            }
            for (int i = 0; i < r_n; i++) {
                threads[i].join();

            }
        } catch (InterruptedException ex){
            ex.printStackTrace();
        }


        System.out.println("Generated " + tmpfiles.size() + " batch files.");

    }
    public static void main(String args[]) {
        //check for input
        if (args.length < 3) {
            System.out.println("Usage [input file] [output file] [number of thread]");
            return;
        }

        try {
            String input_file = args[0];
            String output_file = args[1];

            int r_n = Integer.parseInt(args[2]);
            numOfThread = r_n;
            long startTime = System.nanoTime();


            File input = new File(input_file);
            long fileSize = input.length();
            long bs = MyExternalSort.calculateBlockSize(input);

            //final BufferedReader br = new BufferedReader(new FileReader(input));
            final List<File> tmpfiles = new ArrayList<>();

            partition(input, tmpfiles, r_n, bs);

            long firstSortTime = (System.nanoTime() - startTime) / 1000000000;
            System.out.println("Time on first sort is: " + firstSortTime);
            long freemem = Runtime.getRuntime().freeMemory();
            System.out.println("Free memory during merge is " + freemem / 1024 + " KB.");

            MyExternalSort.mergetmpFiles(tmpfiles, output_file, MyExternalSort.chunk_merge_counter_calculator(freemem, tmpfiles.size()));

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



}


