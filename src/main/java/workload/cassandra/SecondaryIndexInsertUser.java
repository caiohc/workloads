package workload.cassandra;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class SecondaryIndexInsertUser extends CassandraWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "secondaryIndex.counterStart";
	
	private Session session;	
	private AtomicLong keybase;
	private PreparedStatement insertUserStatement;
	
	@Override
	public void init(final Properties properties) {
	
		try {
			super.init(properties);
			session = connectionManager.getSession(CLUSTER);	
			prepareStatements();
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: \n%s\n", e.getMessage());
			System.exit(1);
		}
		
		keybase = new AtomicLong(Long.parseLong(properties.getProperty(COUNTER_START, "1")));		
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final BoundStatement statement = generateInsertUserStatement();
		final long start = System.currentTimeMillis();		
		
		try {
			Submitter.submit(tries, maxBackoff, () -> session.execute(statement));
			Measurements.getMeasurements().measure("INSERT-USER", (int) (System.currentTimeMillis() - start));
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("INSERT-USER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		} 
		
		return true;
		
	}
	
	@Override
	public void prepareStatements() {
		
		final StringBuilder insertUserStatement = new StringBuilder();
		insertUserStatement.append("INSERT INTO user (id, email, ");
		insertUserStatement.append("field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, ");
		insertUserStatement.append("field11, field12, field13, field14, field15, field16, field17, field18, field19, field20) ");
		insertUserStatement.append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		this.insertUserStatement = session.prepare(insertUserStatement.toString());
		
	}
	
	private BoundStatement generateInsertUserStatement() {
		
		final String[] userAttributes = new String[20];
		
		for (int i = 0 ; i < 20 ; i++) {
			userAttributes[i] = RandomDataGenerator.generateData(fieldSize);	
		}
		
		return insertUserStatement.bind(UUID.randomUUID(), Utils.hash(keybase.getAndIncrement()), 
				userAttributes[0], userAttributes[1], userAttributes[2], userAttributes[3], userAttributes[4],
				userAttributes[5], userAttributes[6], userAttributes[7], userAttributes[8], userAttributes[9],
				userAttributes[10], userAttributes[11], userAttributes[12], userAttributes[13], userAttributes[14],
				userAttributes[15], userAttributes[16], userAttributes[17], userAttributes[18], userAttributes[19]);
		
	}

}