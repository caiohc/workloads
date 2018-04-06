package workload.couchbase;

import java.util.Properties;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class IncrementingKey extends CouchbaseWorkload {
	
	private static final String REMOTE_COUNTER = "incrementingKey.remoteCounter";
	private static final String DATA_CLUSTER = "cluster1";
	private static final String COUNTER_CLUSTER = "cluster2";
		
	private boolean isRemoteCounter;
	private Bucket counterBucket;
	private Bucket dataBucket;
	
	@Override
	public void init(final Properties properties) {
	
		isRemoteCounter = Boolean.parseBoolean(properties.getProperty(REMOTE_COUNTER, "false"));
		
		try {
			super.init(properties);
			dataBucket = connectionManager.getBucket(DATA_CLUSTER);
			
			if (isRemoteCounter) {
				counterBucket = connectionManager.getBucket(COUNTER_CLUSTER);
			} else {
				counterBucket = dataBucket;
			}
			
		} catch (WorkloadException e) {
			e.printStackTrace();
		}
		
	}	
	
	@Override
	public boolean doInsert(final DB db, final Object threadState) {
		
		long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, 
					() -> counterBucket.counter("counter::usercounter", 0, 0));
			Measurements.getMeasurements().measure("INSERT-COUNTER", (int) (System.currentTimeMillis() - start));
			return true;
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("INSERT-COUNTER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());
			return false;
		}
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		JsonObject user = generateUserJsonObject();		
		long start = System.currentTimeMillis();
		long usercount = 0;
		
		try {
			usercount = (Long) Submitter.submit(tries, maxBackoff, 
					() -> counterBucket.counter("counter::usercounter", 1).content());
			
			if (usercount == 0) {
				Measurements.getMeasurements().measure("INCREMENT-COUNTER-FAILED", (int) (System.currentTimeMillis() - start));			
				return true;
			} else {
				user.put("user_id", usercount);
				JsonDocument userDocument = JsonDocument.create(String.format("user::%d", usercount), user); 
				Submitter.submit(tries, maxBackoff, () -> dataBucket.insert(userDocument));									
				Measurements.getMeasurements().measure("USER-REGISTRATION", (int) (System.currentTimeMillis() - start));
				return true;
			}
			
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("USER-REGISTRATION-FAILED", (int) (System.currentTimeMillis() - start));
			return true;	
		}
		
	}
	
	private JsonObject generateUserJsonObject() {
		
		final JsonObject user = JsonObject.empty();
		user.put("doctype", "user");
		
		for (int i = 1 ; i <= 20 ; i++) {
			user.put(String.format("field%d", i), RandomDataGenerator.generateData(fieldSize));	
		}
		
		return user;
		
	}

}