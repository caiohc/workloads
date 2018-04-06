package workload.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.inc;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class EnumerableKeysPostComment extends MongoDBWorkload {
	
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
				"enumerableKeys.postUuid", "9d7c9ac5-847f-4688-bb3d-35f8e936a6a6"));
		
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
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
		final long start = System.currentTimeMillis();
		
		try {
			Document postDocument = (Document) Submitter.submit(tries, maxBackoff, () -> 
			mongoDatabase.getCollection("post").findOneAndUpdate(eq("_id", postDocumentUuid), inc("commentCount", 1), options));
			Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("comment")
					.insertOne(generateCommentDocument((Integer) postDocument.get("commentCount"))));
			Measurements.getMeasurements().measure("INSERT-COMMENT", (int) (System.currentTimeMillis() - start));			
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("INSERT-COMMENT-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());			
		}
		
		return true;		
		
	}
	
	private Document generatePostDocument() {
		
		return new Document("_id", postDocumentUuid).append("creationDate", new Date())
				.append("content", RandomDataGenerator.generateData(2000)).append("commentCount", 0);
		
	}
	
	private Document generateCommentDocument(final int count) {
		
		return new Document("_id", String.format("%s:%d", postDocumentUuid, count)).append("creationDate", new Date())
				.append("comment", RandomDataGenerator.generateData(200));
		
	}

}
