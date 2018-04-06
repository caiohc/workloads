package workload.cassandra;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class SecondaryIndexQueryUser extends CassandraWorkload {

	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "secondaryIndex.counterStart";
	
	private Session session;	
	private AtomicLong keybase;
	private PreparedStatement selectUserStatement;
	
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
		
		final long start = System.currentTimeMillis();
		final BoundStatement statement = generateSelectUserStatement();
		
		try {
			final ResultSet result = (ResultSet) Submitter.submit(tries, maxBackoff, () -> session.execute(statement));
			
			if (result.one() != null) {
				Measurements.getMeasurements().measure("QUERY-USER", (int) (System.currentTimeMillis() - start));
			} else {
				Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) (System.currentTimeMillis() - start));
			}
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("INSERT-USER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		} 
		
		return true;
		
	}
	
	@Override
	public void prepareStatements() {
		
		final StringBuilder selectUserStatement = new StringBuilder();
		selectUserStatement.append("SELECT id, email, ");
		selectUserStatement.append("field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, ");
		selectUserStatement.append("field11, field12, field13, field14, field15, field16, field17, field18, field19, field20 ");
		selectUserStatement.append("FROM user WHERE email = ?");		
		this.selectUserStatement = session.prepare(selectUserStatement.toString());
		
	}

	private BoundStatement generateSelectUserStatement() {
		
		return selectUserStatement.bind(Utils.hash(keybase.getAndIncrement()));
		
	}

}