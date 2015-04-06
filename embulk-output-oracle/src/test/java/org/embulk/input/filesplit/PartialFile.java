package org.embulk.input.filesplit;

public class PartialFile
{
    private String path;
    private long start;
    private long end;


    public PartialFile(String path, long start, long end)
    {
        this.path = path;
        this.start = start;
        this.end = end;
    }

    public PartialFile() {
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }


    public long getStart()
    {
        return start;
    }

    public void setStart(long start)
    {
        this.start = start;
    }

    public long getEnd()
    {
        return end;
    }

    public void setEnd(long end)
    {
        this.end = end;
    }
}