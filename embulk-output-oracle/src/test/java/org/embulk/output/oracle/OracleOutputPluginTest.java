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
    private static Object test12c;
    private static Object test11g;

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
            return;
        }

        test11g = createTest("/driver/11g/ojdbc6.jar");
        if (test11g == null) {
            System.out.println("Warning: you should put ojdbc6.jar (version 11g) on 'test/resources/driver/11g' directory.");
            return;
        }
    }

    private static Object createTest(String jdbcDriverPath) throws Exception
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
        Object test = testClass.newInstance();
        invoke(test, "beforeClass");
        return test;
    }

    private static void invoke(String methodName) throws Exception
    {
        invoke(test12c, methodName);
    }

    private static void invoke(Object test, String methodName) throws Exception
    {
        if (test != null) {
            Thread.currentThread().setContextClassLoader(test.getClass().getClassLoader());
            Method method = test.getClass().getMethod(methodName);
            method.invoke(test);
        }
    }

    @Test
    public void testInsert() throws Exception
    {
        invoke("testInsert");
    }

    @Test
    public void testInsertCreate() throws Exception
    {
        invoke("testInsertCreate");
    }

    @Test
    public void testInsertDirect() throws Exception
    {
        // ArrayIndexOutOfBoundsException thrown if using 12c driver.
        invoke(test11g, "testInsertDirect");
    }

    @Test
    public void testInsertOCI() throws Exception
    {
        invoke("testInsertOCI");
    }

    @Test
    public void testInsertOCISplit() throws Exception
    {
        invoke("testInsertOCISplit");
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
    public void testReplaceCreate() throws Exception
    {
        invoke("testReplaceCreate");
    }

}
