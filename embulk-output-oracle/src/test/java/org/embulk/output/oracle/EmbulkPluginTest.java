/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z tiba $
 */
package org.embulk.output.oracle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmbulkPluginTest {
	
	// TODO:assert output
	protected static void execute(String... args) throws Exception{
		for (int i = 0; i < args.length; i++) {
			if (args[i].endsWith(".yml")) {
				args[i] = convert(args[i]);
			}
		}
		
		Class<?> mainClass = EmbulkPluginTest.class.getClassLoader().loadClass("org.embulk.cli.Main");
		Method mainMethod = mainClass.getMethod("main", args.getClass());
		mainMethod.invoke(null, new Object[]{args});
	}
	
	private static File searchEmbulk() throws FileNotFoundException {
		for (String path : System.getProperty("java.library.path").split(System.getProperty("path.separator"))) {
			for (String fileName : new String[]{"embulk", "embulk.bat"}) {
				File file = new File(new File(path), fileName);
				if (file.exists() && file.isFile()) {
					return file;
				}
			}
		}
		throw new FileNotFoundException("Neither embulk nor embulk.bat is found.");
	}
	
	
	private static String convert(String yml) {
		try {
			File rootPath = new File(EmbulkPluginTest.class.getResource("/").toURI());
			File ymlPath = new File(EmbulkPluginTest.class.getResource(yml).toURI());
			File tempYmlPath = new File(ymlPath.getParentFile(), "temp-" + ymlPath.getName());
			Pattern pathPrefixPattern = Pattern.compile("^  path_prefix: '(.*)'$");
			try (BufferedReader reader = new BufferedReader(new FileReader(ymlPath))) {
				try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempYmlPath))) {
					String line;
					while ((line = reader.readLine()) != null) {
						Matcher matcher = pathPrefixPattern.matcher(line);
						if (matcher.matches()) {
							writer.write(line.substring(0, matcher.start(1)));
							writer.write(new File(rootPath, matcher.group(1)).getAbsolutePath());
							writer.write(line.substring(matcher.end(1)));
						} else {
							writer.write(line);
						}
						writer.newLine();
					}
				}
			}
			return tempYmlPath.getAbsolutePath();
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
	
}
