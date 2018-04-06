package workload.couchbase;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class LbaPostComment extends CouchbaseWorkload {

	private static final String CLUSTER = "cluster1";
	
	private Bucket bucket;
	private UUID postDocumentUuid;
	private String postDocumentId;
	private SimpleDateFormat dateFormat;
	
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
		dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		
	}
	
	@Override
	public boolean doInsert(final DB db, final Object threadState) {
		
		final JsonDocument postDocument = generatePostDocument();
		final long start = System.currentTimeMillis();
		
		try {
			Submitter.submit(tries, maxBackoff, () -> bucket.insert(postDocument));
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
		
		final JsonObject commentObject = generateCommentObject();
		final long start = System.currentTimeMillis();
		
		while (true) {
			
			try {
				final JsonDocument post = (JsonDocument) Submitter.submit(tries, maxBackoff, () -> bucket.get(postDocumentId));
				JsonArray comments = post.content().getArray("comments");
				
				if (comments == null) {
					comments = JsonArray.create();
				}
				
				comments.add(commentObject);
				post.content().put("comments", comments);
				Submitter.submit(tries, maxBackoff, () -> bucket.replace(post), true, CASMismatchException.class);
				Measurements.getMeasurements().measure("INSERT-COMMENT", (int) (System.currentTimeMillis() - start));				
			} catch (CASMismatchException exception) {
				continue;
			} catch (SubmitterException exception) {
				Measurements.getMeasurements().measure("INSERT-COMMENT-FAILED", (int) (System.currentTimeMillis() - start));
				System.err.println(exception.getMessage());
			} catch (RuntimeException exception) {
				System.err.printf("Exception: %s\n", exception.getMessage());
				return false;
			}
			
			return true;
			
		}
		
	}
	
	private JsonDocument generatePostDocument() {
		
		final JsonObject post = JsonObject.empty()
				.put("doctype", "post")
				.put("creationDate", dateFormat.format(new Date()))
				.put("content", RandomDataGenerator.generateData(2000));
		
		return JsonDocument.create(postDocumentId, post);
		
	} 
	
	private JsonObject generateCommentObject() {
		
		return  JsonObject.empty().put("doctype", "comment").put("creationDate",  dateFormat.format(new Date())).
				put("comment", RandomDataGenerator.generateData(200));
		
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