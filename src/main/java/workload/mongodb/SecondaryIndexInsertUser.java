package workload.mongodb;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class SecondaryIndexInsertUser extends MongoDBWorkload {
	
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
		final Document userDocument = generateUserDocument();
		final long start = System.currentTimeMillis();		
		
		try {
			Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("user").insertOne(userDocument));
			Measurements.getMeasurements().measure("INSERT-USER", (int) (System.currentTimeMillis() - start));
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("INSERT-USER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		} 
		
		return true;
		
	}
	
	private Document generateUserDocument() {
		
		final String[] userAttributes = new String[20];
		
		for (int i = 0 ; i < 20 ; i++) {
			userAttributes[i] = RandomDataGenerator.generateData(fieldSize);	
		}
		
		return new Document("_id", UUID.randomUUID())
				.append("email", String.valueOf(Utils.hash(keybase.getAndIncrement())))				
				.append("attribute1", userAttributes[0]).append("attribute2", userAttributes[1])
				.append("attribute3", userAttributes[2]).append("attribute4", userAttributes[3])
				.append("attribute5", userAttributes[4]).append("attribute6", userAttributes[5])
				.append("attribute7", userAttributes[6]).append("attribute8", userAttributes[7])
				.append("attribute9", userAttributes[8]).append("attribute10", userAttributes[9])
				.append("attribute11", userAttributes[10]).append("attribute12", userAttributes[11])
				.append("attribute3", userAttributes[12]).append("attribute14", userAttributes[13])
				.append("attribute15", userAttributes[14]).append("attribute16", userAttributes[15])
				.append("attribute17", userAttributes[16]).append("attribute18", userAttributes[17])
				.append("attribute19", userAttributes[18]).append("attribute20", userAttributes[19]);
		
	}

}