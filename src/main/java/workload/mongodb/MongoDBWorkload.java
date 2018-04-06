package workload.mongodb;

import java.util.Properties;

import org.bson.Document;

import com.mongodb.client.MongoDatabase;
import com.yahoo.ycsb.WorkloadException;

import connections.MongoDBConnectionManager;
import util.Submitter;
import util.SubmitterException;
import workload.PatternWorkload;

public abstract class MongoDBWorkload extends PatternWorkload {
	
	private static final String DATABASE = "database";
	private static final String COLLECTIONS = "collections";
	private static final String CLEAN_COLLECTIONS = "cleanCollections";
	
	protected MongoDBConnectionManager connectionManager;
	protected boolean cleanCollections = false;
	protected String[] collections;	
	protected String database; 
	
	@Override
	public void init(final Properties properties) throws WorkloadException {
		
		super.init(properties);
		connectionManager = MongoDBConnectionManager.getInstance();
		
		try {
			connectionManager.init(properties);
			database = properties.getProperty(DATABASE, "tests");
			cleanCollections = Boolean.parseBoolean(properties.getProperty(CLEAN_COLLECTIONS, "false"));
			
			if (cleanCollections) {
				collections = properties.getProperty(COLLECTIONS).split(",");
			}
			
		} catch (Exception e) {
			throw new WorkloadException(e.getMessage());
		}
		
	}

	
	@Override
	public void cleanup() {
		
		if (cleanCollections) {
			boolean error = false;
			
			for (String datacenter : connectionManager.getMongoClients().keySet()) {
				MongoDatabase mongoDatabase = connectionManager.getMongoClient(datacenter).getDatabase(database);
				
				for (String collectionName : collections) {
					
					try {
						Submitter.submit(tries, maxBackoff, 
								() -> mongoDatabase.getCollection(collectionName).deleteMany(new Document()));
					} catch (SubmitterException exception) {
						error = true;
					}
					
				}
				
			}
			
			if (error) {
				System.err.println("It was not possible to truncate the collections.");
			}
			
		}
		
		connectionManager.close();
		
	}	

}