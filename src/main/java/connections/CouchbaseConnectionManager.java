package connections;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

public class CouchbaseConnectionManager implements ConnectionManager {
	
	private static final String CLUSTERS = "clusters";
	private static final String NODES = "nodes";
	private static final String BUCKET = "bucket";
			
	private static CouchbaseConnectionManager instance;
	private Map<String, Cluster> clusters;
	private Map<String, Bucket> buckets;
				
	public static synchronized CouchbaseConnectionManager getInstance() {
		
		if (instance == null) {
			instance = new CouchbaseConnectionManager();
		}
		
		return instance;
		
	}
	
	public void init(final Properties properties) {
		
		final String[] clustersNames = properties.getProperty(CLUSTERS).split(","); 
				
		clusters = new HashMap<>(clustersNames.length);
		buckets = new HashMap<>(clustersNames.length);
		Cluster cluster = null;	
		Bucket bucket = null;
		
		for (String clusterName : clustersNames) {
			cluster = CouchbaseCluster.create(properties.getProperty(String.format("%s.%s", NODES, clusterName)).split(","));
			clusters.put(clusterName, cluster);
			bucket = cluster.openBucket(properties.getProperty(BUCKET, "default"));
			buckets.put(clusterName, bucket);
		}
		
	}
	
	public Bucket getBucket(final String clusterConfig) {
		
		if (buckets == null) {
			throw new IllegalStateException("Connection has not been initialized.");
		} else {
			return buckets.get(clusterConfig);
		}
		
	}
	
	public Map<String, Bucket> getBuckets() {
		
		if (buckets == null) {
			throw new IllegalStateException("Connection has not been initialized.");
		} else {
			return buckets;
		}
		
	}
	
	public void close() {
		
		if (clusters == null) {
			throw new IllegalStateException("Connection has not been initialized or has already been closed.");
		} 
		
		for (String clusterConfig : clusters.keySet()) {
			clusters.get(clusterConfig).disconnect();	
		}
		
		buckets = null;
		clusters = null;
		instance = null;		
		
	}

}