package main;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

public class FileMergeThread extends Thread {
    LinkedList<File> fileList;
    int mergeCount;
    LinkedList<File> resultFileList;
    public FileMergeThread(LinkedList<File> fileList, LinkedList<File> resultFileList, int mergeCount){
        this.fileList = fileList;
        this.resultFileList = resultFileList;
        this.mergeCount = mergeCount;
    }
    @Override
    public void run() {
        LinkedList<File> mergeList = new LinkedList<>();
        // iterate file list, if size of mergeList reach merge count, than start to merge
        while(true){
            synchronized (fileList){
                if(fileList.size() > 0){
                    mergeList.addLast(fileList.removeLast());
                }
            }
            if(mergeList.size() >= mergeCount){
                // user merger to merge file
                startMergeFiles(mergeList);
                mergeList.clear();

            }
            synchronized (fileList){
                if(fileList.size() == 0){
                    break;
                }
            }
        }
        if(mergeList.size() != 0){
            startMergeFiles(mergeList);
            mergeList.clear();
        }

    }

    public void startMergeFiles(LinkedList<File> mergeList){
        try {
            File mergeResult = File.createTempFile("MergeFileChunk", "thread");
            mergeResult.deleteOnExit();
            MyFileMerger merger = new MyFileMerger(mergeList, mergeResult);
            merger.run();
            merger.join();

            // add result to resultFileList
            synchronized (resultFileList){
                resultFileList.addLast(mergeResult);
            }

        } catch (IOException | InterruptedException ex){
            ex.printStackTrace();
        }

    }
}
