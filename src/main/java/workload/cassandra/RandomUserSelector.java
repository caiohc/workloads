package workload.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomUserSelector {
	
	private List<int[]> userActivitiesCounters;
	private int usersLeft;
	private Random random = new Random();
	
	public RandomUserSelector(int usersCount, int activitiesCount) {
		
		this.userActivitiesCounters = new ArrayList<int[]>(usersCount);
		this.usersLeft = usersCount;
		
		for (int i = 0; i < usersCount; i++) {
			userActivitiesCounters.add(new int[] {i + 1, activitiesCount});
		}
		
	}
	
	public synchronized int getRandomUserId() {
		
		int userIndex = random.nextInt(usersLeft);		
		int[] record = userActivitiesCounters.get(userIndex);
		int activitiesCount = record[1];
		
		if (--activitiesCount == 0) {
			userActivitiesCounters.remove(userIndex);
			usersLeft--;
		} else {
			record[1] = activitiesCount;
			userActivitiesCounters.set(userIndex, record);
		}
		
		return record[0];
		
	}

}