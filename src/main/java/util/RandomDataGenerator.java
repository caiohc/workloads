package util;

import com.yahoo.ycsb.RandomByteIterator;

public class RandomDataGenerator {
	
	public static String generateData(final int datalength) {
		
		return new RandomByteIterator(datalength).toString();
		
	}

}