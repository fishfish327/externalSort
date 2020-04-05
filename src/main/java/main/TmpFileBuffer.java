package main;

import java.io.*;

public class TmpFileBuffer {
    public BufferedReader fbr;
    public File originalfile;
    private String myLine;
    private boolean empty;

    public TmpFileBuffer(File f) throws IOException {
        originalfile = f;
        fbr = new BufferedReader(new FileReader(f), 1024 * 8);
        fetch();
    }

    public boolean empty() {
        return empty;
    }

    private void fetch() throws IOException {
        try {
            if ((this.myLine = fbr.readLine()) == null) {
                empty = true;
                myLine = null;
            } else {
                empty = false;
            }
        } catch (EOFException oef) {
            empty = true;
            myLine = null;
        }
    }

    public void close() throws IOException {
        fbr.close();
    }


    public String peek() {
        if (empty()) return null;
        return myLine.toString();
    }

    public String pop() throws IOException {
        String answer = peek();
        fetch();
        return answer;
    }

}