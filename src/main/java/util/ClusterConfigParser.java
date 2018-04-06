package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

public class ClusterConfigParser {
	
	public static Map<String, List<String>> parseClusterConfig(final Properties properties, final String datacenterProperty,
			final String datacenterNodesProperty) {
		
		final StringTokenizer datacentersTokenizer = new StringTokenizer(properties.getProperty(datacenterProperty), ",");
		final Map<String, List<String>> datacenters = new HashMap<String, List<String>>(datacentersTokenizer.countTokens());
		
		while (datacentersTokenizer.hasMoreTokens()) {
			final String datacenter = datacentersTokenizer.nextToken();
			final StringTokenizer nodesTokenizer = new StringTokenizer(properties.getProperty(
					String.format("%1$s.%2$s", datacenterNodesProperty, datacenter)), ",");
			final List<String> nodes = new ArrayList<String>(nodesTokenizer.countTokens());
			
			while (nodesTokenizer.hasMoreTokens()) {
				nodes.add(nodesTokenizer.nextToken());
			}
			
			datacenters.put(datacenter, nodes);
		}
		
		return datacenters;
		
	}

}