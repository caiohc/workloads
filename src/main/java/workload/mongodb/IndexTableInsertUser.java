package workload.mongodb;

import java.util.HashMap;
import java.util.Map;
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

public class IndexTableInsertUser extends MongoDBWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String COUNTER_START = "indexTable.counterStart";
	
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
		final Map<String, Document> documents = generateDocuments();
		Document userDocument = documents.get("user");
		Document emailDocument = documents.get("email");
		final long start = System.currentTimeMillis();		
		
		try {
			Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("user").insertOne(userDocument));
			Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("email").insertOne(emailDocument));
			Measurements.getMeasurements().measure("INSERT-USER", (int) (System.currentTimeMillis() - start));
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("INSERT-USER-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		} 
		
		return true;
		
	}
	
	private Map<String, Document> generateDocuments() {
		
		final Map<String, Document> data = new HashMap<String, Document>(2);
		final UUID userId = UUID.randomUUID();
		final String email = String.valueOf(Utils.hash(keybase.getAndIncrement()));
		Document userDocument = new Document("_id", userId).append("email", email);
		
		for (int i = 1 ; i <= 20 ; i++) {
			userDocument.append(String.format("%s%d", "attribute", i), RandomDataGenerator.generateData(fieldSize));	
		}
				
		data.put("user", userDocument);
		data.put("email", new Document("_id", email).append("user_id", userId));
		
		return data;
		
	}

}