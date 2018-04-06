package connections;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnectionManager implements ConnectionManager {
	
	private static final String CLUSTERS = "clusters";
	private static final String NODES = "nodes";
	private static final String KEYSPACE = "keyspace";
	
	private static CassandraConnectionManager instance;
	private Map<String, Cluster> clusters;	
	private Map<String, Session> sessions;
		
	public static synchronized CassandraConnectionManager getInstance() {
		
		if (instance == null) {
			instance = new CassandraConnectionManager();
		}
		
		return instance;
		
	}
	
	public void init(final Properties properties) {
		
		final String[] clustersNames = properties.getProperty(CLUSTERS).split(",");
		
		clusters = new HashMap<>(clustersNames.length);
		sessions = new HashMap<>(clustersNames.length);
		Cluster cluster = null;
		Session session = null;
		
		for (String clusterName : clustersNames) {
			cluster = Cluster.builder().addContactPoints(
					properties.getProperty(String.format("%s.%s", NODES, clusterName)).split(",")).build();
			clusters.put(clusterName, cluster);
			session = cluster.connect(properties.getProperty(KEYSPACE));	
			sessions.put(clusterName, session);			
		}		
		
	}
	
	public Session getSession(final String clusterName) {
		
		if (sessions == null) {
			throw new IllegalStateException("Connection has not been initialized.");
		} else {
			return sessions.get(clusterName);
		}			
			
	}
	
	public Map<String, Session> getSessions() {
		
		if (sessions == null) {
			throw new IllegalStateException("Connection has not been initialized.");
		} else {
			return sessions;
		}			
			
	}
	
	public void close() {
		
		if (clusters == null) {
			throw new IllegalStateException("Connection has not been initialized or has already been closed.");
		} 			
		
		for (String clusterName : clusters.keySet()) {
			clusters.get(clusterName).close();	
		}
		
		sessions = null;
		clusters = null;
		instance = null;
		
	}	
	
}