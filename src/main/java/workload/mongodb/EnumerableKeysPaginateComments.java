package workload.mongodb;

import static com.mongodb.client.model.Filters.in;

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

public class EnumerableKeysPaginateComments extends MongoDBWorkload {
	
	private static final String CLUSTER = "cluster1";
	
	private MongoClient mongoClient;
	private UUID postDocumentUuid;
	private int commentCount;
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
		
		postDocumentUuid = UUID.fromString(properties.getProperty(
				"enumerableKeys.postUuid", "9d7c9ac5-847f-4688-bb3d-35f8e936a6a6"));
		final String commentCount = properties.getProperty("enumerableKeys.pagination.commentCount");
		
		if (commentCount == null) {
			System.err.println("Error: index of last comment was not set.");
			System.exit(1);
		} else {
			this.commentCount = Integer.parseInt(commentCount);
		}
		
	}
	
	@Override
	public Object initThread(final Properties properties, final int mythreadid, final int threadcount) throws WorkloadException {
		
		Map<String, Integer> paginationState = new HashMap<String, Integer>();
		paginationState.put("index", random.nextInt(commentCount) / 10 * 10);
		return paginationState;
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		final Map<String, Integer> paginationState = (Map<String, Integer>) threadState; 
		final int index = paginationState.get("index");
		final MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		final String uuid = postDocumentUuid.toString();
		final long start = System.currentTimeMillis();
		
		try {
			((FindIterable<Document>) Submitter.submit(tries, maxBackoff, () -> mongoDatabase.getCollection("comment").find(in("_id", 
				String.format("%s:%d", uuid, index + 1), String.format("%s:%d", uuid, index + 2),
				String.format("%s:%d", uuid, index + 3), String.format("%s:%d", uuid, index + 4),
				String.format("%s:%d", uuid, index + 5), String.format("%s:%d", uuid, index + 6),
				String.format("%s:%d", uuid, index + 7), String.format("%s:%d", uuid, index + 8),
				String.format("%s:%d", uuid, index + 9), String.format("%s:%d", uuid, index + 10))))).first();			
			Measurements.getMeasurements().measure("QUERY-COMMENTS", (int) (System.currentTimeMillis() - start));						
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("QUERY-COMMENTS-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());
		}
		
		if ((index + 10) < commentCount) {
			paginationState.put("index", index + 10);
		} else {
			paginationState.put("index", 0);
		}
		
		return true;
		
	}

}