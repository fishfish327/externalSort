package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class MyFileMerger extends Thread {
    List<File> tmpfiles;
    File outfile;
    List<TmpFileBuffer> bufferList;
    /*public MyFileMerger(List<File> tmpfiles, File outfile) {
        this.tmpfiles = tmpfiles;
        this.outfile = outfile;
    } */

    public MyFileMerger(List<TmpFileBuffer> bufferList, File outfile){
        this.bufferList = bufferList;
        this.outfile = outfile;
    }

    @Override
    public void run() {
        super.run();

        PriorityQueue<TmpFileBuffer> pq = new PriorityQueue<TmpFileBuffer>(11,
                new Comparator<TmpFileBuffer>() {
                    public int compare(TmpFileBuffer i, TmpFileBuffer j) {
                        String a = i.peek();
                        String b = j.peek();
                        if (a == null || b == null) {
                            return 0;
                        } else {
                            return a.compareTo(b);
                        }

                    }
                }
        );

        try {
            for (TmpFileBuffer buffer : bufferList) {
                // set buffer size
                pq.add(buffer);
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(outfile));
            while (pq.size() > 0) {
                TmpFileBuffer tfb = pq.poll();
                String r = tfb.pop();
                writer.write(r + "\r\n");
                if (tfb.empty()) {
                    tfb.close();
                    tfb.originalfile.delete();
                } else {
                    pq.add(tfb);
                }
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
