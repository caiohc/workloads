package workload.couchbase;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Bucket;
import com.yahoo.ycsb.WorkloadException;

import connections.CouchbaseConnectionManager;
import util.Submitter;
import util.SubmitterException;
import workload.PatternWorkload;

public abstract class CouchbaseWorkload extends PatternWorkload {
	
	private static final String FLUSH_BUCKET = "flushBucket";
	private static final String FLUSH_BUCKET_TIMEOUT = "flushBucketTimeout";
			
	protected CouchbaseConnectionManager connectionManager;
	protected boolean flushBucket;
	protected long flushBucketTimeout;	
			
	@Override
	public void init(final Properties properties) throws WorkloadException {
		
		super.init(properties);
		connectionManager = CouchbaseConnectionManager.getInstance();
		
		try {
			connectionManager.init(properties);
			flushBucket = Boolean.parseBoolean(properties.getProperty(FLUSH_BUCKET, "false"));
			flushBucketTimeout = Long.parseLong(properties.getProperty(FLUSH_BUCKET_TIMEOUT, "300"));
		} catch (Exception e) {
			throw new WorkloadException(e.getMessage());
		}
		
	}
	
	@Override
	public void cleanup() {
		
		if (flushBucket) {
			
			if (!flushBucket()) {
				System.err.println("It was not possible to flush the buckets.");
			}
			
		}	
			
		connectionManager.close();
		
	}	
	
	protected boolean flushBucket() {
		
		boolean error = false;
	
		for (String clusterName : connectionManager.getBuckets().keySet()) {
			Bucket bucket = connectionManager.getBucket(clusterName);
			
			try {
				Submitter.submit(tries, maxBackoff, () -> bucket.bucketManager().flush(flushBucketTimeout, TimeUnit.SECONDS));				
			} catch (SubmitterException exception) {
				error = false;
			}
		}
		
		return error;
		
	}

}