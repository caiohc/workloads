package workload.couchbase;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class SecondaryIndexQueryUser extends CouchbaseWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "secondaryIndex.counterStart";
	
	private Bucket bucket;
	private AtomicLong keybase;
	private String statement;
	
	@Override
	public void init(final Properties properties) {
	
		try {
			super.init(properties);
			bucket = connectionManager.getBucket(CLUSTER);
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: \n%s\n", e.getMessage());
			System.exit(1);
		}
		
		keybase = new AtomicLong(Long.parseLong(properties.getProperty(COUNTER_START, "1")));
		statement = "SELECT id, email, field1, field2, field3, field4, field5, field6, field7, field8, " +
				"field9, field10, field11, field12, field13, field14, field15, field16, field17, field18, field19, field20 " +
				"FROM `default` WHERE email = $1";
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final N1qlQuery query = prepareQuery();
		final long start = System.currentTimeMillis();
		
		try {
			final N1qlQueryResult result = (N1qlQueryResult) Submitter.submit(tries, maxBackoff, () -> bucket.query(query));
			
			final long elapsedTime = System.currentTimeMillis() - start;
			
			if (result.info().resultCount() > 0) {
				Measurements.getMeasurements().measure("QUERY-USER", (int) elapsedTime);
			} else {
				Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) elapsedTime);
			}
			
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());
		} catch (RuntimeException exception) {
			System.err.printf("Exception: %s\n", exception.getMessage());
			return false;
		}
		
		return true;
		
	}

	private N1qlQuery prepareQuery() {
		
		return N1qlQuery.parameterized(statement, JsonArray.from(Long.toString(Utils.hash(keybase.getAndIncrement()))), 
				N1qlParams.build().adhoc(true));
		
	}
	
}