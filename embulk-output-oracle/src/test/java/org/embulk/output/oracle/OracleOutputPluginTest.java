package org.embulk.output.oracle;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.embulk.input.filesplit.LocalFileSplitInputPlugin;
import org.embulk.output.tester.EmbulkPluginTester;
import org.embulk.plugin.PluginClassLoader;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.junit.BeforeClass;
import org.junit.Test;


public class OracleOutputPluginTest
{
    private static class NamedObject
    {
        public final String name;
        public final Object value;
        public NamedObject(String name, Object value)
        {
            this.name = name;
            this.value = value;
        }
    }

    private static List<NamedObject> testObjects = new ArrayList<NamedObject>();
    private static NamedObject test12c;
    private static NamedObject test11g;
    private static EmbulkPluginTester tester = new EmbulkPluginTester();


    @BeforeClass
    public static void beforeClass() throws Exception
    {
        if (System.getProperty("path.separator").equals(";")) {
            // for Windows
            System.setProperty("file.encoding", "MS932");
        }

        tester.addPlugin(InputPlugin.class, "filesplit", LocalFileSplitInputPlugin.class);

        test12c = createTest("oracle12", "/driver/12c/ojdbc7.jar");
        if (test12c == null) {
            System.out.println("Warning: you should put ojdbc7.jar (version 12c) on 'test/resources/driver/12c' directory.");
        } else {
            testObjects.add(test12c);
        }

        test11g = createTest("oracle11", "/driver/11g/ojdbc6.jar");
        if (test11g == null) {
            System.out.println("Warning: you should put ojdbc6.jar (version 11g Release 2) on 'test/resources/driver/11g' directory.");
        } else {
            testObjects.add(test11g);
        }
    }

    private static NamedObject createTest(String pluginName, String jdbcDriverPath) throws Exception
    {
        // Load OracleOutputPluginTestImpl, Oracle JDBC Driver, embulk-output-oracle by another ClassLoader
        // in order to test for different driver versions.

        List<URL> urls = new ArrayList<URL>();

        File testRoot = new File(OracleOutputPluginTest.class.getResource("/dummy.txt").toURI()).getParentFile();
        String pluginClassName = "org.embulk.output.OracleOutputPlugin";
        URL pluginClassUrl = OracleOutputPluginTest.class.getResource("/"  +pluginClassName.replace('.', '/') + ".class");
        File root = new File(pluginClassUrl.toURI()).getParentFile().getParentFile().getParentFile().getParentFile();

        urls.add(root.toURI().toURL());
        urls.add(testRoot.toURI().toURL());

        URL jdbcDriverUrl = OracleOutputPluginTest.class.getResource(jdbcDriverPath);
        if (jdbcDriverUrl == null) {
            return null;
        }

        urls.add(jdbcDriverUrl);
        ClassLoader classLoader = new PluginClassLoader(urls, OracleOutputPluginTest.class.getClassLoader(),
                Arrays.asList(EmbulkPluginTester.class.getPackage().getName()), new ArrayList<String>());
        Thread.currentThread().setContextClassLoader(classLoader);
        tester.addPlugin(OutputPlugin.class, pluginName, classLoader.loadClass(pluginClassName));

        Class<?> testClass = classLoader.loadClass(OracleOutputPluginTest.class.getName() + "Impl");
        final Object testObject = testClass.newInstance();
        invoke(testObject, "setTester", tester);
        invoke(testObject, "setPluginName", pluginName);

        final String version = (String)invoke(testObject, "beforeClass");
        if (version == null) {
            return null;
        }
        return new NamedObject(version, testObject);
    }

    private static void invoke(String methodName) throws Exception
    {
        for (NamedObject testObject : testObjects) {
            invoke(testObject, methodName);
        }
    }

    private static Object invoke(NamedObject testObject, String methodName) throws Exception
    {
        if (testObject != null) {
            System.out.println("*** " + testObject.name + " ***");
            return invoke(testObject.value, methodName);
        }
        return null;
    }

    private static Object invoke(Object testObject, String methodName, Object... arguments) throws Exception
    {
        if (testObject != null) {
            Thread.currentThread().setContextClassLoader(testObject.getClass().getClassLoader());
            Class<?>[] parameterTypes = new Class<?>[arguments.length];
            for (int i = 0; i < arguments.length; i++) {
                parameterTypes[i] = arguments[i].getClass();
            }
            Method method = testObject.getClass().getMethod(methodName, parameterTypes);
            return method.invoke(testObject, arguments);
        }
        return null;
    }

    @Test
    public void testInsert() throws Exception
    {
        invoke("testInsert");
    }

    @Test
    public void testInsertEmpty() throws Exception
    {
        invoke("testInsertEmpty");
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        invoke("testInsertDirect");
    }

    @Test
    public void testInsertDirectCreate() throws Exception
    {
        invoke("testInsertDirectCreate");
    }

    @Test
    public void testInsertDirectEmpty() throws Exception
    {
        invoke("testInsertDirectEmpty");
    }

    @Test
    public void testInsertDirectDirectMethod() throws Exception
    {
        // ArrayIndexOutOfBoundsException thrown if using 12c driver.
        invoke(test11g, "testInsertDirectDirectMethod");
    }

    @Test
    public void testInsertDirectOCIMethod() throws Exception
    {
        invoke("testInsertDirectOCIMethod");
    }

    @Test
    public void testInsertDirectOCIMethodSplit() throws Exception
    {
        invoke("testInsertDirectOCIMethodSplit");
    }

    @Test
    public void testUrl() throws Exception
    {
        invoke("testUrl");
    }

    @Test
    public void testReplace() throws Exception
    {
        invoke("testReplace");
    }

    @Test
    public void testReplaceEmpty() throws Exception
    {
        invoke("testReplaceEmpty");
    }

    @Test
    public void testReplaceLongName() throws Exception
    {
        invoke("testReplaceLongName");
    }

    @Test
    public void testReplaceLongNameMultibyte() throws Exception
    {
        invoke("testReplaceLongNameMultibyte");
    }

    @Test
    public void testReplaceCreate() throws Exception
    {
        invoke("testReplaceCreate");
    }

    @Test
    public void testStringTimestamp() throws Exception
    {
        invoke("testStringTimestamp");
    }
}
