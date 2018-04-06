package workload.couchbase;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class IndexTableQueryUser extends CouchbaseWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "indexTable.counterStar";
	
	private Bucket bucket;
	private AtomicLong queryKeybase;
	
	@Override
	public void init(final Properties properties) {
		
		try {
			super.init(properties);
			bucket = connectionManager.getBucket(CLUSTER);
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: %s\n", e.getMessage());
			System.exit(1);
		}
		
		queryKeybase = new AtomicLong(Long.parseLong(properties.getProperty(COUNTER_START, "1")));
		
	}

	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final String email = String.format("email::%s", Utils.hash(queryKeybase.getAndIncrement()));
		final long start = System.currentTimeMillis();
		
		try {
			final StringDocument userId = 
					(StringDocument) Submitter.submit(tries, maxBackoff, () ->  bucket.get(email, StringDocument.class));
			final JsonDocument user = (JsonDocument) Submitter.submit(tries, maxBackoff, () -> bucket.get(userId.content()));
			final long elapsedTime = System.currentTimeMillis() - start; 
			
			if (user.content() != null) {
				Measurements.getMeasurements().measure("QUERY-USER", (int) elapsedTime);
			} else {
				Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) elapsedTime);
			}
			
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("QUERY-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());
			return false;
		}
		
		return true;
		
	}

}