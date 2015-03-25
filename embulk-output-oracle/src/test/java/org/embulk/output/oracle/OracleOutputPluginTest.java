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
    private static Object impl;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        if (System.getProperty("path.separator").equals(";")) {
            // forw Windows
            System.setProperty("file.encoding", "MS932");
        }

        String[] classPaths = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        List<URL> urls = new ArrayList<URL>();
        for (String classPath : classPaths) {
            urls.add(new File(classPath).toURI().toURL());
        }
        // load Oracle JDBC driver dynamically, in order to enable to build without the driver.
        URL url = OracleOutputPluginTest.class.getResource("/driver/ojdbc6.jar");
        if (url == null) {
            System.out.println("Warning: you should put ojdbc6.jar on 'test/resources/driver' directory.");
            return;
        }

        urls.add(url);
        ClassLoader classLoader = new ChildFirstClassLoader(urls, OracleOutputPluginTest.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(classLoader);

        Class<?> implClass = classLoader.loadClass(OracleOutputPluginTest.class.getName() + "Impl");
        impl = implClass.newInstance();
        invoke("beforeClass");
    }

    private static void invoke(String methodName) throws Exception
    {
        if (impl != null) {
            Method method = impl.getClass().getMethod(methodName);
            method.invoke(impl);
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
        invoke("testInsertDirect");
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
