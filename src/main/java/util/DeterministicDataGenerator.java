package util;

public class DeterministicDataGenerator {
	
	public static String generateData(final String prefix, final int datalength) {
	    
		final StringBuilder sb = new StringBuilder(datalength);
	    sb.append(prefix);
	    
	    while (sb.length() < datalength) {
	      sb.append(':');
	      sb.append(sb.toString().hashCode());
	    }
	    
	    sb.setLength(datalength);

	    return sb.toString();
	    
	}

}