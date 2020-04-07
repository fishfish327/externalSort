package common;

public class FileSlice {
    public final long begin;
    public final long end;

    public FileSlice(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }
}
