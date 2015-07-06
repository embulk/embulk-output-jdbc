package org.embulk.output.oracle;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

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

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        if (System.getProperty("path.separator").equals(";")) {
            // forw Windows
            System.setProperty("file.encoding", "MS932");
        }

        test12c = createTest("/driver/12c/ojdbc7.jar");
        if (test12c == null) {
            System.out.println("Warning: you should put ojdbc7.jar (version 12c) on 'test/resources/driver/12c' directory.");
        } else {
            testObjects.add(test12c);
        }

        test11g = createTest("/driver/11g/ojdbc6.jar");
        if (test11g == null) {
            System.out.println("Warning: you should put ojdbc6.jar (version 11g Release 2) on 'test/resources/driver/11g' directory.");
        } else {
            testObjects.add(test11g);
        }
    }

    private static NamedObject createTest(String jdbcDriverPath) throws Exception
    {
        String[] classPaths = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        List<URL> urls = new ArrayList<URL>();
        for (String classPath : classPaths) {
            urls.add(new File(classPath).toURI().toURL());
        }
        // load Oracle JDBC driver dynamically, in order to enable to build without the driver.
        URL url = OracleOutputPluginTest.class.getResource(jdbcDriverPath);
        if (url == null) {
            return null;
        }

        urls.add(url);
        ClassLoader classLoader = new ChildFirstClassLoader(urls, OracleOutputPluginTest.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(classLoader);

        Class<?> testClass = classLoader.loadClass(OracleOutputPluginTest.class.getName() + "Impl");
        final Object testObject = testClass.newInstance();
        final String version = (String)invoke(testObject, "beforeClass");
        if (version == null) {
            return null;
        }
        return new NamedObject(version, testObject);
    }

    private static void invoke(String methodName) throws Exception
    {
        //invoke(test12c, methodName);
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

    private static Object invoke(Object testObject, String methodName) throws Exception
    {
        if (testObject != null) {
            Thread.currentThread().setContextClassLoader(testObject.getClass().getClassLoader());
            Method method = testObject.getClass().getMethod(methodName);
            return method.invoke(testObject);
        }
        return null;
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
