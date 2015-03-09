package org.embulk.output.oracle;


public class MySQLCreateTest extends EmbulkPluginTest {
	
	public static void main(String[] args) throws Exception {
		execute("run", "/yml/test-mysql-create.yml");
	}
	
}
