package org.embulk.output.oracle;


public class CreateTest extends EmbulkPluginTest {
	
	public static void main(String[] args) {
		execute("run", "/yml/test-create.yml");
	}
	
}
