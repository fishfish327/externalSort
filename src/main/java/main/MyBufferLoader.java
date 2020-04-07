package main;

import java.io.File;
import java.util.List;
import java.util.Queue;

public class MyBufferLoader extends Thread {
    Queue<File> fileQueue;
    List<TmpFileBuffer> bufferList;
    long bufferSize;
    public MyBufferLoader(Queue<File> fileQueue, List<TmpFileBuffer> bufferList, long bufferSize){
        this.fileQueue = fileQueue;
        this.bufferList = bufferList;
        this.bufferSize = bufferSize;
    }
    @Override
    public void run(){
        while (true){
            File tmp = null;
            TmpFileBuffer tmpFileBuffer = null;
            synchronized (fileQueue){
                if(fileQueue.size() > 0){
                    tmp = fileQueue.poll();
                }
            }
            if(tmp == null){
                break;
            } else {
                tmpFileBuffer = new TmpFileBuffer(tmp, bufferSize);
            }
            synchronized (bufferList){
                bufferList.add(tmpFileBuffer);
            }

        }

    }
}
