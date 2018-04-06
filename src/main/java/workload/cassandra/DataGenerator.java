package workload.cassandra;

public class DataGenerator {
	
	private long userStart;
	private long currentUser;
	private long itemStart;
	private long itemCount;
	private long currentItem;
	private int itemsPerUser;
	private int itemsPerUserCounter; 
	
	public DataGenerator(long userStart, long itemStart, long itemCount, int itemsPerUser) {
	
		super();
		this.userStart = userStart;
		this.currentUser = 0;
		this.itemStart = itemStart;
		this.itemCount = itemCount;
		this.currentItem = 0;
		this.itemsPerUser = itemsPerUser;
		this.itemsPerUserCounter = 0;
		
	}
	
	public synchronized long[] getData() {
		
		return new long[]{getUser(), getItem()};
		
	}
	
	private long getUser() {
		
		if (itemsPerUserCounter < itemsPerUser) {
			itemsPerUserCounter++;
		} else {
			itemsPerUserCounter = 1;
			currentUser++;
		}
		
		return userStart + currentUser;
		
	}
	
	private long getItem() {
		
		long item = 0;
		
		if (currentItem < itemCount) {
			item = currentItem;			
		} else {
			currentItem = 0;
			item = currentItem;
		}
		currentItem++;
		
		return itemStart + item;
		
	}
	
}