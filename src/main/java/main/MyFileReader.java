package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class MyFileReader extends Thread {
    BufferedReader br;
    long max_capacity;
    List<File> file_list;
    int thread_no;

    public MyFileReader(BufferedReader br, long max, List<File> file_list, int thread_no) {
        this.br = br;
        max_capacity = max;
        this.file_list = file_list;
        this.thread_no = thread_no;
    }

    @Override
    public void run() {
        String line = "";
        List<String> lines = new ArrayList<>();
        long c = 0;
        try {

            while (line != null) {
                synchronized (br) {
                    line = br.readLine();
                    if (line != null && !line.equals("")) {
                        c += line.length() + 20; //length + overhead
                        lines.add(line);

                        if (c >= max_capacity) {
                            File tmpfile = saveTmpFile(lines);
                            c = 0;
                            lines.clear();
                            synchronized (file_list) {
                                file_list.add(tmpfile);
                            }
                        }

                    }
                }
            }

            if (lines.size() > 0) {
                File tmpfile = saveTmpFile(lines);
                lines.clear();
                synchronized (file_list) {
                    file_list.add(tmpfile);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //save file chunk as temporary file
    public File saveTmpFile(List<String> lines) {
        Collections.sort(lines, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });

        BufferedWriter fbw;
        try {
            File tmpfile = File.createTempFile("FileChunk", "Thread.tmp");
            tmpfile.deleteOnExit();

            fbw = new BufferedWriter(new FileWriter(tmpfile), 1024 * 8);
            for (String r : lines) {
                fbw.write(r + "\r\n");
            }
            fbw.close();
            return tmpfile;
        } catch (Exception e) {
            return null;
        }
    }
}
