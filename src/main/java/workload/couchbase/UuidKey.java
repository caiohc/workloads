package workload.couchbase;

import java.util.Properties;
import java.util.UUID;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class UuidKey extends CouchbaseWorkload {
	
	private static final String CLUSTER = "cluster1";
	
	private Bucket bucket;
	
	@Override
	public void init(final Properties properties) {
		
		try {
			super.init(properties);
			bucket = connectionManager.getBucket(CLUSTER);
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: %s\n", e.getMessage());
			System.exit(1);
		}		
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final JsonDocument userDocument = generateUserJsonDocument();
		final long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, () -> bucket.insert(userDocument));	
			Measurements.getMeasurements().measure("USER-REGISTRATION", (int) (System.currentTimeMillis() - start));
			return true;
		}  catch (SubmitterException e) {
			Measurements.getMeasurements().measure("USER-REGISTRATION-FAILED", (int) (System.currentTimeMillis() - start));
			return true;
		}		
		
	}
		
	private JsonDocument generateUserJsonDocument() {
		
		final String userUuid = UUID.randomUUID().toString();
		final JsonObject user = JsonObject.empty();
		user.put("doctype", "user");
		user.put("user_id", userUuid);
		
		for (int i = 1 ; i <= 20 ; i++) {
			user.put(String.format("field%d", i), RandomDataGenerator.generateData(fieldSize));	
		}
		
		return JsonDocument.create(String.format("user::%s", userUuid), user);
		
	}

}