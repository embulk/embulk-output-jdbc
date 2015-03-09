package org.embulk.output.oracle;


public class ReplaceTest extends EmbulkPluginTest {
	
	public static void main(String[] args) throws Exception {
		execute("run", "/yml/test-replace.yml");
	}
	
}
