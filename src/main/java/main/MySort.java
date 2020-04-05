package main;

import sort.SortFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MySort {
    public static List<String> generateFile(String fileName, int n) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        File inputFile = new File(fileName);
        long inputFileSize = inputFile.length();
        long outputFileSize = inputFileSize / n;
        long totalRead = 0;
        List<String> res = new ArrayList<>();
        for(int i = 0; i < n; i++){
            String tmpFileName = fileName + "-output-" + i;
            char[] buffer = new char[(int) outputFileSize];
            reader.read(buffer,(int)totalRead, (int)outputFileSize );
            SortFunction.quickSort(buffer);
            File outputFile = new File(tmpFileName);
            outputFile.createNewFile();
            FileWriter writer = new FileWriter(outputFile);
            writer.write(buffer);
            writer.close();
            res.add(tmpFileName);
            System.out.println("Generate file : " + fileName + "with size of: " + outputFileSize);

        }
        return res;

    }

    public void sortFiles(List<String> fileList, int bufferSize, String outputFileName) throws IOException {
        int num = fileList.size();
        List<ReadBufferNode> readBufferNodeList = new ArrayList<>();
        for(int i = 0; i < num; i++){
            readBufferNodeList.add(new ReadBufferNode(bufferSize, fileList.get(i)));
        }
        // create output buffer node
        WriteBufferNode outputBuffer = new WriteBufferNode(bufferSize, outputFileName);

        while(true){
            boolean flag = false;
            int min = 128;
            int minIndex = -1;
            for(int i = 0; i < num; i++){
                ReadBufferNode currentNode = readBufferNodeList.get(i);
                if(currentNode.index == currentNode.numOfElements){
                    currentNode.readBlock();
                }
                if(currentNode.isComplete()){
                    continue;
                }
                int digit = currentNode.elements[currentNode.index] - '\0';
                if(digit < min){
                    min = digit;
                    minIndex = i;
                }
            }
            if(minIndex != -1){
                char next = (char)(min + '\0');
                outputBuffer.writeChar(next);
                readBufferNodeList.get(minIndex).index ++;
                if(outputBuffer.index == outputBuffer.size){
                    outputBuffer.flushToFile();
                }
                flag = true;

            }
            if(flag == false){
                break;
            }

        }
        // close all sortNode
        for(ReadBufferNode node : readBufferNodeList){
            node.closeFile();
        }
    }
    public static void main(String[] args){


    }
}
