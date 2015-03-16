package org.embulk.output.oracle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.embulk.EmbulkService;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ExecutionResult;
import org.embulk.exec.LocalExecutor;
import org.embulk.plugin.InjectedPluginSource;
import org.embulk.spi.ExecSession;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;

public class EmbulkPluginTester
{

    private final Class<?> iface;
    private final String name;
    private final Class<?> impl;


    public EmbulkPluginTester(Class<?> iface, String name, Class<?> impl)
    {
        this.iface = iface;
        this.name = name;
        this.impl = impl;
    }

    public void run(String ymlPath) throws Exception
    {
        EmbulkService service = new EmbulkService(new EmptyConfigSource()) {
            protected Iterable<? extends Module> getAdditionalModules(ConfigSource systemConfig)
            {
                return Arrays.asList(new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        InjectedPluginSource.registerPluginTo(binder, iface, name, impl);
                    }
                });
            }
        };
        Injector injector = service.getInjector();
        ConfigSource config = injector.getInstance(ConfigLoader.class).fromYamlFile(new File(ymlPath));
        ExecSession session = new ExecSession(injector, config);
        LocalExecutor executor = injector.getInstance(LocalExecutor.class);
        ExecutionResult result = executor.run(session, config);
    }

    private File convert(String yml) {
        try {
            File rootPath = new File(EmbulkPluginTester.class.getResource("/resource.txt").toURI()).getParentFile();
            File ymlPath = new File(EmbulkPluginTester.class.getResource(yml).toURI());
            File tempYmlPath = new File(ymlPath.getParentFile(), "temp-" + ymlPath.getName());
            Pattern pathPrefixPattern = Pattern.compile("^ *path(_prefix)?: '(.*)'$");
            try (BufferedReader reader = new BufferedReader(new FileReader(ymlPath))) {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempYmlPath))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Matcher matcher = pathPrefixPattern.matcher(line);
                        if (matcher.matches()) {
                            int group = 2;
                            writer.write(line.substring(0, matcher.start(group)));
                            writer.write(new File(rootPath, matcher.group(group)).getAbsolutePath());
                            writer.write(line.substring(matcher.end(group)));
                        } else {
                            writer.write(line);
                        }
                        writer.newLine();
                    }
                }
            }
            return tempYmlPath.getAbsoluteFile();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}
