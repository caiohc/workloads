package workload.mongodb;

import static com.mongodb.client.model.Filters.eq;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class SecondaryIndexQueryUser extends MongoDBWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "secondaryIndex.counterStart";
	
	private MongoClient mongoClient;
	private AtomicLong keybase;
	
	@Override	
	public void init(final Properties properties) {
		
		try {
			super.init(properties);
			mongoClient = connectionManager.getMongoClient(CLUSTER);
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: \n%s\n", e.getMessage());
			System.exit(1);
		}
		
		keybase = new AtomicLong(Long.parseLong(properties.getProperty(COUNTER_START, "1")));
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		final String email = String.valueOf(Utils.hash(keybase.getAndIncrement())); 
		final long start = System.currentTimeMillis();		
		
		try {
			Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("user").find(eq("email", email)).first());
			Measurements.getMeasurements().measure("QUERY-USER", (int) (System.currentTimeMillis() - start));
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("QUERY-USER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		} 
		
		return true;
		
	}

}