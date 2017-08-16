package org.embulk.output.oracle;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

public class DummyPath implements Path
{
    private final Path original;
    private final String fileContents;

    public DummyPath(Path original, String fileContents)
    {
        this.original = original;
        this.fileContents = fileContents;
    }

    @Override
    public int compareTo(Path other)
    {
        return original.compareTo(other);
    }

    @Override
    public boolean endsWith(Path other)
    {
        return original.endsWith(other);
    }

    @Override
    public boolean endsWith(String other)
    {
        return original.endsWith(other);
    }

    @Override
    public Path getFileName()
    {
        return original.getFileName();
    }

    @Override
    public FileSystem getFileSystem()
    {
        return new DummyFileSystem(original.getFileSystem(), fileContents);
    }

    @Override
    public Path getName(int index)
    {
        return original.getName(index);
    }

    @Override
    public int getNameCount()
    {
        return original.getNameCount();
    }

    @Override
    public Path getParent()
    {
        return original.getParent();
    }

    @Override
    public Path getRoot()
    {
        return original.getRoot();
    }

    @Override
    public boolean isAbsolute()
    {
        return original.isAbsolute();
    }

    @Override
    public Iterator<Path> iterator()
    {
        return original.iterator();
    }

    @Override
    public Path normalize()
    {
        return original.normalize();
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>... events) throws IOException
    {
        return original.register(watcher, events);
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) throws IOException
    {
        return original.register(watcher, events, modifiers);
    }

    @Override
    public Path relativize(Path other)
    {
        return original.relativize(other);
    }

    @Override
    public Path resolve(Path other)
    {
        return original.resolve(other);
    }

    @Override
    public Path resolve(String other)
    {
        return original.resolve(other);
    }

    @Override
    public Path resolveSibling(Path other)
    {
        return original.resolveSibling(other);
    }

    @Override
    public Path resolveSibling(String other)
    {
        return original.resolveSibling(other);
    }

    @Override
    public boolean startsWith(Path other)
    {
        return original.startsWith(other);
    }

    @Override
    public boolean startsWith(String other)
    {
        return original.startsWith(other);
    }

    @Override
    public Path subpath(int beginIndex, int endIndex)
    {
        return original.subpath(beginIndex, endIndex);
    }

    @Override
    public Path toAbsolutePath()
    {
        return original.toAbsolutePath();
    }

    @Override
    public File toFile()
    {
        return original.toFile();
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException
    {
        return original.toRealPath(options);
    }

    @Override
    public URI toUri()
    {
        return original.toUri();
    }

    @Override
    public String toString()
    {
        return original.toString();
    }

}
