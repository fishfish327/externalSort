package main;

import java.io.*;

public class TmpFileBuffer {
    public BufferedReader fbr;
    public File originalfile;
    private String myLine;
    private String[] lines;
    private int capacity;
    private int index;
    private int size;
    private boolean empty;

    public TmpFileBuffer(File f, int bufferNum, int bufferSize) throws IOException {
        originalfile = f;
        fbr = new BufferedReader(new FileReader(f), bufferSize);
        this.capacity = bufferNum;
        lines = new String[capacity];
        fetch();
    }

    public boolean empty() {
        return empty;
    }

    private void fetch() throws IOException {
        try {
            // fetch data into string array
            this.size = 0;
            this.index = 0;
            for(int i = 0; i < lines.length; i++){
                lines[i] = fbr.readLine();
                if(lines[i] == null){
                    break;
                }
                size ++;
            }
            if(size > 0){
                myLine = lines[index];
                empty = false;
            } else {
                myLine = null;
                empty = true;
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
        return myLine;
    }

    public String pop() throws IOException {
        String answer = peek();
        this.index ++;
        if(this.index >= this.size){
            fetch();
        } else {
            this.myLine = lines[this.index];
        }
        return answer;
    }

}