package main;

import java.io.*;

public class WriteBufferNode {
    public char[] elements;
    public int size;
    public int index;
    public int numOfElements;
    public String fileName;
    public int totalWrite;

    public BufferedWriter bufferedWriter;

    public WriteBufferNode(int size, String fileName) throws IOException {
        elements = new char[size];
        this.size = size;
        this.fileName = fileName;
        this.bufferedWriter = new BufferedWriter(new FileWriter(fileName));

    }
    public void writeChar(char c){
        elements[index++] = c;
        numOfElements ++;
    }
    public void flushToFile() throws IOException {
        bufferedWriter.write(elements, totalWrite, numOfElements);
        totalWrite += numOfElements;
        index = 0;
        numOfElements = 0;
    }

    public void closeFile() throws IOException {
        this.bufferedWriter.close();
    }
}
