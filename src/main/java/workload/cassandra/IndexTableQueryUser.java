package workload.cassandra;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class IndexTableQueryUser extends CassandraWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "indexTable.counterStart";
	
	private Session session;	
	private AtomicLong keybase;
	private PreparedStatement selectUserStatement;
	private PreparedStatement selectUserEmailStatement;
	
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
		
		BoundStatement selectEmailStatement = generateSelectEmailStatement();
		final long start = System.currentTimeMillis();
		
		try {
			final Row emailRecord = ((ResultSet) Submitter.submit(
					 tries, maxBackoff, () -> session.execute(selectEmailStatement))).one();
			
			if (emailRecord != null) {
				final ResultSet result = session.execute(selectUserStatement.bind(emailRecord.getUUID(1)));
				
				if (result.one() != null) {
					Measurements.getMeasurements().measure("QUERY-USER", (int) (System.currentTimeMillis() - start));
				} else {
					Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) (System.currentTimeMillis() - start));
				}

			} else {
				Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) (System.currentTimeMillis() - start));
			}
			
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) (System.currentTimeMillis() - start));
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
		selectUserStatement.append("FROM user WHERE id = ?");		
		this.selectUserStatement = session.prepare(selectUserStatement.toString());
		
		final StringBuilder selectUserEmailStatement = new StringBuilder();
		selectUserEmailStatement.append("SELECT email, user_id FROM email WHERE email = ?");		
		this.selectUserEmailStatement = session.prepare(selectUserEmailStatement.toString());
		
	}
	
	private BoundStatement generateSelectEmailStatement() {
		
		return selectUserEmailStatement.bind(Utils.hash(keybase.getAndIncrement()));
		
	}

}
