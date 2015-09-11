package org.embulk.input.filesplit;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;

import com.google.common.base.Optional;


public class LocalFileSplitInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("path")
        public String getPath();

        @Config("tasks")
        @ConfigDefault("null")
        public Optional<Integer> getTasks();

        @Config("header_line")
        @ConfigDefault("false")
        public boolean getHeaderLine();

        public List<PartialFile> getFiles();
        public void setFiles(List<PartialFile> files);

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        int tasks;
        if (task.getTasks().isPresent()) {
            tasks = task.getTasks().get();
            if (tasks <= 0) {
                throw new IllegalArgumentException(String.format("'tasks' is %d but must be greater than 0", tasks));
            }
        } else {
            tasks = Runtime.getRuntime().availableProcessors() * 2;
        }

        long size = new File(task.getPath()).length();
        List<PartialFile> files = new ArrayList<PartialFile>();
        for (int i = 0; i < tasks; i++) {
            long start = size * i / tasks;
            long end = size * (i + 1) / tasks;
            if (start < end) {
                files.add(new PartialFile(task.getPath(), start, end));
            }
        }

        task.setFiles(files);

        return resume(task.dump(), task.getFiles().size(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successCommitReports)
    { }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new LocalFileSplitInput(task, taskIndex);
    }

    public static class LocalFileSplitInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public static class FileSplitProvider
                implements InputStreamFileInput.Provider
        {
            private final PartialFile file;
            private final boolean hasHeader;
            private boolean opened = false;

            public FileSplitProvider(PartialFile file, boolean hasHeader)
            {
                this.file = file;
                this.hasHeader = hasHeader;
            }

            @Override
            public InputStream openNext() throws IOException
            {
                if (opened) {
                    return null;
                }
                opened = true;

                InputStream in = new PartialFileInputStream(new FileInputStream(file.getPath()), file.getStart(), file.getEnd());
                if (file.getStart() > 0 && hasHeader) {
                    in = new SequenceInputStream(openHeader(file.getPath()), in);
                }
                return in;
            }

            @Override
            public void close() { }

            private InputStream openHeader(String path) throws IOException
            {
                ByteArrayOutputStream header = new ByteArrayOutputStream();
                try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(path))) {
                    while (true) {
                        int c = in.read();
                        if (c < 0) {
                            break;
                        }

                        header.write(c);

                        if (c == '\n') {
                            break;
                        }

                        if (c == '\r') {
                            int c2 = in.read();
                            if (c2 == '\n') {
                                header.write(c2);
                            }
                            break;
                        }
                    }
                }
                header.close();
                return new ByteArrayInputStream(header.toByteArray());
            }
        }

        public LocalFileSplitInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new FileSplitProvider(task.getFiles().get(taskIndex), task.getHeaderLine()));
        }

        @Override
        public void abort() { }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }
    }
}
