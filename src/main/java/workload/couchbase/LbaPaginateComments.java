package workload.couchbase;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class LbaPaginateComments extends CouchbaseWorkload{

	private static final String CLUSTER = "cluster1";
	
	private Bucket bucket;
	private UUID postDocumentUuid;
	private String postDocumentId;
	private int commentCount;
	private Random random = new Random();
	
	@Override
	public void init(final Properties properties) {
	
		try {
			super.init(properties);
			bucket = connectionManager.getBucket(CLUSTER);
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: \n%s\n", e.getMessage());
			System.exit(1);
		}
		
		postDocumentUuid = UUID.fromString(properties.getProperty("lba.postUuid", "5682c7f7-ce2a-4133-b99c-f9a3a9cd884a"));
		postDocumentId = String.format("post::%s", postDocumentUuid);
		final String commentCount = properties.getProperty("lba.pagination.commentCount");
		
		if (commentCount == null) {
			System.err.println("Error: index of last comment was not set.");
			System.exit(1);
		} else {
			this.commentCount = Integer.parseInt(commentCount);
		}
		
	}
	
	@Override
	public Object initThread(final Properties properties, final int mythreadid, final int threadcount) throws WorkloadException {
		
		Map<String, Integer> paginationState = new HashMap<String, Integer>(1);
		paginationState.put("index", random.nextInt(commentCount) / 10 * 10);
		return paginationState;
		
	}
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) {
		
		@SuppressWarnings("unchecked")
		final Map<String, Integer> paginationState = (Map<String, Integer>) threadState; 
		final int index = paginationState.get("index");
		final long start = System.currentTimeMillis();
		
		try {
			final JsonDocument post = (JsonDocument) Submitter.submit(tries, maxBackoff, () -> bucket.get(postDocumentId));
			post.content().getArray("comments");
			Measurements.getMeasurements().measure("GET-COMMENTS", (int) (System.currentTimeMillis() - start));			
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("GET-COMMENTS-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());			
		} catch (RuntimeException exception) {
			System.err.printf("Exception: %s\n", exception.getMessage());
			return false;
		}
		
		if ((index + 10) < commentCount) {
			paginationState.put("index", index + 10);
		} else {
			paginationState.put("index", 0);
		}
		
		return true;
		
	}
	
	@Override
	protected boolean flushBucket() {
		
		try {
			Submitter.submit(tries, maxBackoff, () -> bucket.remove(postDocumentId));
			return true;
		} catch (SubmitterException exception) {
			return false;
		}
		
	}
	
}