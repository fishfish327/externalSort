package common;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;

public class IOUtil {

    public static boolean skipNextLine(RandomAccessFile raf) throws IOException {
        boolean eol = false;
        boolean eof = false;
        while (!eol && !eof) {
            switch (raf.read()) {
                case -1:
                    eof = true;
                    break;
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long curr = raf.getFilePointer();
                    if ((raf.read()) != '\n') {
                        raf.seek(curr);
                    }
                    break;
            }
        }

        return eol;
    }

    public static void closeQuietly(Reader reader) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }

    public static void closeQuietly(Writer writer) {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }

    public static void closeQuietly(RandomAccessFile raf) {
        try {
            if (raf != null) {
                raf.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }

}
