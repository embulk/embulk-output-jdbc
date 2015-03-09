package org.embulk.output.oracle;


public class MySQLInsertTest extends EmbulkPluginTest {
	
	public static void main(String[] args) throws Exception {
		execute("run", "/yml/test-mysql-insert.yml");
	}
	
}
