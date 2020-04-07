package main;

import java.io.*;

public class TmpFileBuffer {
    public BufferedReader fbr;
    public File originalfile;
    private String myLine;
    private boolean empty;

    public TmpFileBuffer(File f, long bufferSize) {
        originalfile = f;
        try {
            fbr = new BufferedReader(new FileReader(f),(int) bufferSize);
            fetch();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

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