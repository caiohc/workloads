package workload.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.slice;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class LbaPaginateComments extends MongoDBWorkload {
	
	private static final String CLUSTER = "cluster1";
	
	private MongoClient mongoClient;
	private UUID postDocumentUuid;	
	private int lastCommentIndex;
	private Random random = new Random();
			
	@Override
	public void init(final Properties properties) {
		
		try {
			super.init(properties);
			mongoClient = connectionManager.getMongoClient(CLUSTER);
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: \n%s\n", e.getMessage());
			System.exit(1);
		}
		
		postDocumentUuid = UUID.fromString(properties.getProperty("lba.postUuid", "5682c7f7-ce2a-4133-b99c-f9a3a9cd884a"));
		final String lastCommentIndex = properties.getProperty("lba.pagination.lastCommentIndex");
		
		if (lastCommentIndex == null) {
			System.err.println("Error: index of last comment was not set.");
			System.exit(1);
		} else {
			this.lastCommentIndex = Integer.parseInt(lastCommentIndex);
		}
		
	}
	
	@Override
	public Object initThread(final Properties properties, final int mythreadid, final int threadcount) throws WorkloadException {
		
		Map<String, Integer> paginationState = new HashMap<String, Integer>();
		paginationState.put("index", random.nextInt(lastCommentIndex) / 10 * 10);
		return paginationState;
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final Map<String, Integer> paginationState = (Map<String, Integer>) threadState; 
		final int index = paginationState.get("index");
		final MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		final long start = System.currentTimeMillis();
		
		try {
			((FindIterable<Document>) 
					Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("post")
				.find(eq("_id", postDocumentUuid)).projection(slice("comments", index, 10)))).first();
			Measurements.getMeasurements().measure("QUERY-COMMENTS", (int) (System.currentTimeMillis() - start));			
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("QUERY-COMMENTS-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());			
		}
		
		if ((index + 10) < lastCommentIndex) {
			paginationState.put("index", index + 10);
		} else {
			paginationState.put("index", 0);
		}
		
		return true;
		
	}

}