package connections;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoDBConnectionManager implements ConnectionManager {
	
	private static final String CLUSTERS = "clusters";
	private static final String NODES = "nodes";	
	
	private static MongoDBConnectionManager instance;
	private Map<String, MongoClient> mongoClients;
		
	public static synchronized MongoDBConnectionManager getInstance() {
		
		if (instance == null) {
			instance = new MongoDBConnectionManager();
		}
		
		return instance;
		
	}
	
	public void init(final Properties properties) throws Exception {
		
		final String[] clustersNames = properties.getProperty(CLUSTERS).split(",");
		mongoClients = new HashMap<String, MongoClient>(clustersNames.length);
		
		String[] nodes = null;
		String[] connectionData = null;
		
		for (String clusterName : clustersNames) {
			nodes = properties.getProperty(String.format("%s.%s", NODES, clusterName)).split(",");
			List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>(nodes.length);
			
			for (String node : nodes) {
				connectionData = node.split(":");
				serverAddresses.add(new ServerAddress(connectionData[0], Integer.parseInt(connectionData[1])));				
			}
			
			mongoClients.put(clusterName, new MongoClient(serverAddresses));	
		}
		
	}
	
	public MongoClient getMongoClient(final String datacenter) {
		
		if (mongoClients == null) {
			throw new IllegalStateException("Connection has not been initialized.");
		} else {
			return mongoClients.get(datacenter);
		}
		
	}
	
	public Map<String, MongoClient> getMongoClients() {
		
		if (mongoClients == null) {
			throw new IllegalStateException("Connection has not been initialized.");
		} else {
			return mongoClients;
		}
		
	}
	
	public void close() {
	
		if (mongoClients == null) {
			throw new IllegalStateException("Connection has not been initialized or has already been closed.");
		}
		
		for (String datacenter : mongoClients.keySet()) {
			mongoClients.get(datacenter).close();
		}
		
		instance = null;
		
	}

}