package connections;

import java.util.Properties;

public interface ConnectionManager {
	
	public void init(final Properties properties) throws Exception;
	
	public void close();

}