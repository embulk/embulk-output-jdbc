package org.embulk.output.oracle;


public class InsertTest extends EmbulkPluginTest {
	
	public static void main(String[] args) throws Exception {
		execute("run", "/yml/test-insert.yml");
	}
	
}
