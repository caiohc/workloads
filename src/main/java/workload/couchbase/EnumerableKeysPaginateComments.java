package workload.couchbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import rx.Observable;
import rx.functions.Func1;
import util.Submitter;
import util.SubmitterException;

public class EnumerableKeysPaginateComments extends CouchbaseWorkload {

private static final String CLUSTER = "cluster1";
	
	private Bucket bucket;
	private UUID postDocumentUuid;
	private int commentCount;
	private String commentIdPrefix;
	private Random random = new Random();
	
	@Override
	public void init(final Properties properties) {
	
		try {
			super.init(properties);
			bucket = connectionManager.getBucket(CLUSTER);
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: %s\n", e.getMessage());
			System.exit(1);
		}
		
		postDocumentUuid = UUID.fromString(properties.getProperty(
				"enumerableKeys.postUuid", "9d7c9ac5-847f-4688-bb3d-35f8e936a6a6"));
		final String commentCount = 
				properties.getProperty("enumerableKeys.pagination.commentCount");
		
		if (commentCount == null) {
			System.err.println("Error: index of last comment was not set.");
			System.exit(1);
		} else {
			this.commentCount = Integer.parseInt(commentCount);
		}
		
		commentIdPrefix = String.format("comment::%s:", postDocumentUuid.toString());
		
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
		
		final List<String> commentIds = new ArrayList<String>();
		
		for (int i = 1; i <= 10; i++) {
			commentIds.add(String.format("%s%d", commentIdPrefix, index + i));
		}
		
		final long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, () -> bulkGet(commentIds));
			Measurements.getMeasurements().measure("GET-COMMENTS", (int) (System.currentTimeMillis() - start));			
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("GET-COMMENTS-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());			
		}
		
		if ((index + 10) < commentCount) {
			paginationState.put("index", index + 10);
		} else {
			paginationState.put("index", 0);
		}
		
		return true;
		
	}
	
	private List<JsonDocument> bulkGet(final List<String> ids) {
		
		return Observable.from(ids).flatMap(new Func1<String, Observable<JsonDocument>>() {
				@Override
				public Observable<JsonDocument> call(String id) {
					return bucket.async().get(id);
				}
		    }).toList().toBlocking().single();
		
	}
	
}