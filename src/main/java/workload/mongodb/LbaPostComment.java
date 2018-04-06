package workload.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class LbaPostComment extends MongoDBWorkload {
	
	private static final String CLUSTER = "cluster1";
	
	private MongoClient mongoClient;
	private UUID postDocumentUuid;		
		
	@Override
	public void init(final Properties properties) {
		
		try {
			super.init(properties);
			mongoClient = connectionManager.getMongoClient(CLUSTER);			
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: \n%s\n", e.getMessage());
			System.exit(1);
		}
		
		postDocumentUuid = UUID.fromString(properties.getProperty(
				"lba.postUuid", "5682c7f7-ce2a-4133-b99c-f9a3a9cd884a"));
		
	}
	
	@Override
	public boolean doInsert(final DB db, final Object threadState) {
		
		final MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		final Document postDocument = generatePostDocument();
		final long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("post").insertOne(postDocument));
			Measurements.getMeasurements().measure("INSERT-POST", (int) (System.currentTimeMillis() - start));
			return true;
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("INSERT-POST-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());
			return false;
		}
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		final Document commentDocument = generateCommentDocument();
		final long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("post").updateOne(
					eq("_id", postDocumentUuid), push("comments", commentDocument)));			
			Measurements.getMeasurements().measure("INSERT-COMMENT", (int) (System.currentTimeMillis() - start));			
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("INSERT-POST-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());
		}
		
		return true;
		
	}
	
	private Document generatePostDocument() {
		
		return new Document("_id", postDocumentUuid).append("creationDate", new Date())
				.append("content", RandomDataGenerator.generateData(2000));
		
	}
	
	private Document generateCommentDocument() {
		
		return new Document("creationDate", new Date()).append("comment", RandomDataGenerator.generateData(200));
		
	}	

}