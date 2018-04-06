package workload.cassandra;

import java.util.Properties;

import com.datastax.driver.core.Session;
import com.yahoo.ycsb.WorkloadException;

import connections.CassandraConnectionManager;
import util.Submitter;
import util.SubmitterException;
import workload.PatternWorkload;

public abstract class CassandraWorkload extends PatternWorkload {
	
	
	private static final String TABLES = "tables";
	private static final String TRUNCATE_TABLES = "truncateTables";
	
	protected CassandraConnectionManager connectionManager;
	protected String[] tables;
	protected boolean truncateTables = false;
	
	@Override
	public void init(final Properties properties) throws WorkloadException {
		
		super.init(properties);
		connectionManager = CassandraConnectionManager.getInstance();
		
		try {
			connectionManager.init(properties);
			truncateTables = Boolean.parseBoolean(properties.getProperty(TRUNCATE_TABLES, "false"));			
						
			if (truncateTables) {
				tables = properties.getProperty(TABLES).split(",");
			}
			
		} catch (Exception e) {
			throw new WorkloadException(e.getMessage());
		}
		
	}

	@Override
	public void cleanup() { 
	
		if (truncateTables) {
			boolean error = false;
			
			for (String datacenter : connectionManager.getSessions().keySet()) {
				Session session = connectionManager.getSession(datacenter);
				
				for (String table : tables) {
					
					try {
						Submitter.submit(tries, 1000, () -> session.execute(String.format("TRUNCATE TABLE %s", table)));
					} catch (SubmitterException e) {
						error = true;
					}
					
				}
				
			}
			
			if (error) {
				System.err.println("It was not possible to truncate the tables.");
			}
		}
		
		connectionManager.close();
	
	}
	
	public abstract void prepareStatements();
	
}