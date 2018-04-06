package workload.couchbase;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class SecondaryIndexInsertUser extends CouchbaseWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "secondaryIndex.counterStart";
	
	private Bucket bucket;
	private AtomicLong keybase;	
	
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
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final JsonDocument userDocument = generateUserJsonDocument();		
		final long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, () -> bucket.upsert(userDocument));
			Measurements.getMeasurements().measure("INSERT-USER", (int) (System.currentTimeMillis() - start));
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("INSERT-USER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());
		} catch (RuntimeException exception) {
			System.err.printf("Exception: %s\n", exception.getMessage());
			return false;
		}
		
		
		return true;
		
	}
	
	private JsonDocument generateUserJsonDocument() {
		
		final JsonObject user = JsonObject.empty().put("doctype", "user")
				.put("email", Long.toString(Utils.hash(keybase.getAndIncrement())));
		
		for (int i = 1 ; i <= 20 ; i++) {
			user.put(String.format("field%d", i), RandomDataGenerator.generateData(fieldSize));	
		}
		
		return JsonDocument.create(String.format("user::%s", UUID.randomUUID().toString()), user);
		
	}

}
