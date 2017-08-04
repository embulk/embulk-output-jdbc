package org.embulk.output.oracle;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

public class DummyFileSystem extends FileSystem
{
    private final FileSystem original;
    private final String fileContents;

    public DummyFileSystem(FileSystem original, String fileContents)
    {
        this.original = original;
        this.fileContents = fileContents;
    }

    @Override
    public FileSystemProvider provider()
    {
        return new DummyFileSystemProvider(original.provider(), fileContents);
    }

    @Override
    public void close() throws IOException
    {
        original.close();
    }

    @Override
    public boolean isOpen()
    {
        return original.isOpen();
    }

    @Override
    public boolean isReadOnly()
    {
        return original.isReadOnly();
    }

    @Override
    public String getSeparator()
    {
        return original.getSeparator();
    }

    @Override
    public Iterable<Path> getRootDirectories()
    {
        return original.getRootDirectories();
    }

    @Override
    public Iterable<FileStore> getFileStores()
    {
        return original.getFileStores();
    }

    @Override
    public Set<String> supportedFileAttributeViews()
    {
        return original.supportedFileAttributeViews();
    }

    @Override
    public Path getPath(String first, String... more)
    {
        return original.getPath(first, more);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern)
    {
        return original.getPathMatcher(syntaxAndPattern);
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService()
    {
        return original.getUserPrincipalLookupService();
    }

    @Override
    public WatchService newWatchService() throws IOException
    {
        return original.newWatchService();
    }

}
