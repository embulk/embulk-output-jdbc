package org.embulk.output.tester;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.embulk.EmbulkEmbed;
import org.embulk.EmbulkEmbed.Bootstrap;
import org.embulk.config.ConfigSource;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.GuessPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;

public class EmbulkPluginTester
{
    public static class PluginDefinition
    {
        public final Class<?> iface;
        public final String name;
        public final Class<?> impl;


        public PluginDefinition(Class<?> iface, String name, Class<?> impl)
        {
            this.iface = iface;
            this.name = name;
            this.impl = impl;
        }

    }

    private final LinkedHashMap<Class<?>, ArrayList<PluginDefinition>> plugins;

    private EmbulkEmbed embulk;

    public EmbulkPluginTester()
    {
        this.plugins = new LinkedHashMap<>();
        this.plugins.put(DecoderPlugin.class, new ArrayList<>());
        this.plugins.put(EncoderPlugin.class, new ArrayList<>());
        this.plugins.put(ExecutorPlugin.class, new ArrayList<>());
        this.plugins.put(FileInputPlugin.class, new ArrayList<>());
        this.plugins.put(FileOutputPlugin.class, new ArrayList<>());
        this.plugins.put(FilterPlugin.class, new ArrayList<>());
        this.plugins.put(FormatterPlugin.class, new ArrayList<>());
        this.plugins.put(GuessPlugin.class, new ArrayList<>());
        this.plugins.put(InputPlugin.class, new ArrayList<>());
        this.plugins.put(OutputPlugin.class, new ArrayList<>());
        this.plugins.put(ParserPlugin.class, new ArrayList<>());
    }

    public EmbulkPluginTester(Class<?> iface, String name, Class<?> impl)
    {
        this();
        addPlugin(iface, name, impl);
    }

    public void addPlugin(Class<?> iface, String name, Class<?> impl)
    {
        this.plugins.get(iface).add(new PluginDefinition(iface, name, impl));
    }

    public List<PluginDefinition> getOutputPlugins()
    {
        return Collections.unmodifiableList(this.plugins.get(OutputPlugin.class));
    }

    @SuppressWarnings("unchecked")
    public void run(String yml) throws Exception
    {
        if (embulk == null) {
            Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
            for (final PluginDefinition plugin : this.plugins.get(DecoderPlugin.class)) {
                bootstrap.builtinDecoderPlugin(plugin.name, (Class<DecoderPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(EncoderPlugin.class)) {
                bootstrap.builtinEncoderPlugin(plugin.name, (Class<EncoderPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(ExecutorPlugin.class)) {
                bootstrap.builtinExecutorPlugin(plugin.name, (Class<ExecutorPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(FileInputPlugin.class)) {
                bootstrap.builtinFileInputPlugin(plugin.name, (Class<FileInputPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(FileOutputPlugin.class)) {
                bootstrap.builtinFileOutputPlugin(plugin.name, (Class<FileOutputPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(FilterPlugin.class)) {
                bootstrap.builtinFilterPlugin(plugin.name, (Class<FilterPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(FormatterPlugin.class)) {
                bootstrap.builtinFormatterPlugin(plugin.name, (Class<FormatterPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(GuessPlugin.class)) {
                bootstrap.builtinGuessPlugin(plugin.name, (Class<GuessPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(InputPlugin.class)) {
                bootstrap.builtinInputPlugin(plugin.name, (Class<InputPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(OutputPlugin.class)) {
                bootstrap.builtinOutputPlugin(plugin.name, (Class<OutputPlugin>) plugin.impl);
            }
            for (final PluginDefinition plugin : this.plugins.get(ParserPlugin.class)) {
                bootstrap.builtinParserPlugin(plugin.name, (Class<ParserPlugin>) plugin.impl);
            }
            embulk = bootstrap.initialize();
        }
        ConfigSource config = embulk.newConfigLoader().fromYamlString(yml);
        embulk.run(config);
    }

    public void destroy()
    {
        if (embulk != null) {
            embulk.destroy();
            embulk = null;
        }
    }

}
