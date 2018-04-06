package workload.couchbase;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
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

public class EnumerableKeysPostComment extends CouchbaseWorkload {

	private static final String CLUSTER = "cluster1";
	
	private Bucket bucket;
	private UUID postDocumentUuid;
	private String postDocumentId;
	private String commentCountId;
	private SimpleDateFormat dateFormat;	
	
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
		postDocumentId = String.format("post::%s", postDocumentUuid);
		commentCountId = String.format("commentcount::%s", postDocumentId);
		dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		
	}
	
	@Override
	public boolean doInsert(final DB db, final Object threadState) {
		
		final JsonDocument postDocument = generatePostDocument();
		final long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, () -> bucket.insert(postDocument));
			Submitter.submit(tries, maxBackoff, () -> bucket.counter(commentCountId, 0, 0));
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
		
		final long start = System.currentTimeMillis();
		
		try {
			final long commentCount = (Long) Submitter.submit(tries, maxBackoff, () -> bucket.counter(commentCountId, 1).content());
			Submitter.submit(tries, maxBackoff, () -> bucket.insert(generateCommentDocument(commentCount)));
			Measurements.getMeasurements().measure("INSERT-COMMENT", (int) (System.currentTimeMillis() - start));
		} catch (SubmitterException exception) {
			Measurements.getMeasurements().measure("INSERT-COMMENT-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(exception.getMessage());
		}
		
		return true;
		
	}
	
	private JsonDocument generatePostDocument() {
		
		final JsonObject post = JsonObject.empty()
				.put("doctype", "post")
				.put("creationDate", dateFormat.format(new Date()))
				.put("content", RandomDataGenerator.generateData(2000));
		
		return JsonDocument.create(postDocumentId, post);
		
	}
	
	private JsonDocument generateCommentDocument(final long count) {
		
		final JsonObject comment = JsonObject.empty()
				.put("doctype", "comment")
				.put("creationDate",  dateFormat.format(new Date()))
				.put("comment", RandomDataGenerator.generateData(200)); 
		
		return JsonDocument.create(String.format("comment::%s:%d", postDocumentUuid.toString(), count), comment);
		
	}
	
}