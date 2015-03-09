package org.embulk.output.oracle;


public class MySQLReplaceTest extends EmbulkPluginTest {
	
	public static void main(String[] args) throws Exception {
		execute("run", "/yml/test-mysql-replace.yml");
	}
	
}
