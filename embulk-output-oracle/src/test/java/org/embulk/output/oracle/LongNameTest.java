package org.embulk.output.oracle;


public class LongNameTest extends EmbulkPluginTest {
	
	public static void main(String[] args) throws Exception {
		execute("run", "/yml/test-long-name.yml");
	}
	
}
