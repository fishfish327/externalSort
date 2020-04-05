package main;

import java.io.*;

public class ReadBufferNode {
    public char[] elements;
    public int size;
    public int index;
    public int numOfElements;
    public String fileName;
    public int totalRead;
    public boolean complete;

    public BufferedReader bufferedReader;


    public ReadBufferNode(int size, String fileName) throws IOException {
        elements = new char[size];
        this.size = size;
        this.fileName = fileName;
        this.bufferedReader = new BufferedReader(new FileReader(fileName));


    }

    public void readBlock() throws IOException {
        int count = bufferedReader.read(elements, totalRead, size);
        if(count == 0){
            this.complete = true;
        }
        this.numOfElements = count;
        index = 0;
        totalRead += count;

    }

    // if there are more data to read in file
    public boolean isComplete(){
        return this.complete;
    }

    public void closeFile() throws IOException {
        this.bufferedReader.close();
    }
}
