package org.embulk.output.oracle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

public class DummyFileSystemProvider extends FileSystemProvider
{
    private final FileSystemProvider original;
    private final String fileContents;

    public DummyFileSystemProvider(FileSystemProvider original, String fileContents)
    {
        this.original = original;
        this.fileContents = fileContents;
    }

    @Override
    public String getScheme()
    {
        return original.getScheme();
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException
    {
        return original.newFileSystem(uri, env);
    }

    @Override
    public FileSystem getFileSystem(URI uri)
    {
        return original.getFileSystem(uri);
    }

    @Override
    public Path getPath(URI uri)
    {
        return original.getPath(uri);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException
    {
        return original.newByteChannel(path, options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter)
            throws IOException
    {
        return original.newDirectoryStream(dir, filter);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException
    {
        original.createDirectory(dir, attrs);
    }

    @Override
    public void delete(Path path) throws IOException
    {
        original.delete(path);
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException
    {
        original.copy(source, target, options);
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException
    {
        original.move(source, target, options);
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException
    {
        return original.isSameFile(path, path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException
    {
        return original.isHidden(path);
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException
    {
        return original.getFileStore(path);
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException
    {
        original.checkAccess(path, modes);
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options)
    {
        return original.getFileAttributeView(path, type, options);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
            throws IOException
    {
        return original.readAttributes(path, type, options);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
            throws IOException
    {
        return original.readAttributes(path, attributes, options);
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
            throws IOException
    {
        original.setAttribute(path, attribute, value, options);
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException
    {
        return new ByteArrayInputStream(fileContents.getBytes());
    }

}
