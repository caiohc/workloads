package workload.cassandra;

import java.util.HashMap;
import java.util.Map;

class ForPaginationStateHolder {
	
	private Map<Long, Long> lastPagedFriendsIds;
	private Map<Long, Integer> pagedFriendsCounts;
	private Map<Long, Map<Long, Long>> lastPagedActivitiesTimes;
	private Map<Long, Integer> pagedActivitiesCounts;
	private int friendsPerUser;
	private boolean[] usersLock;
	
	public ForPaginationStateHolder(int userCount, int friendsPerUser) {
		
		this.lastPagedFriendsIds = new HashMap<>(userCount);
		this.pagedFriendsCounts = new HashMap<>(userCount);
		this.lastPagedActivitiesTimes = new HashMap<>(userCount);
		this.pagedActivitiesCounts = new HashMap<>(userCount);
		this.friendsPerUser = friendsPerUser;
		this.usersLock = new boolean[userCount + 1]; //Index 0 is not used.
		
	}
	
	public boolean lockUser(int userId) { 
	
		if (usersLock[userId]) { //If the user is already locked...
			return false; //...refuse locking request.
		} else { //If the user is not locked...
			usersLock[userId] = true; //...lock user...
			return true; //...confirm lock request.
		}
		
	}
	
	public void unlockUser(int userId) { usersLock[userId] = false; } 
	
	public void setLastPagedFriendId(long userId, long lastPagedFriendId) {
		
		lastPagedFriendsIds.put(userId, lastPagedFriendId);		
				
	}
	
	public void setLastPagedActivityTime(long userId, long friendId, long lastPagedActivityTime) {
		
		Map<Long, Long> friendsPaginationState = lastPagedActivitiesTimes.get(userId);
		
		if (friendsPaginationState == null) { 
			friendsPaginationState = new HashMap<>(friendsPerUser);
			lastPagedActivitiesTimes.put(userId, friendsPaginationState);
		}
		
		friendsPaginationState.put(friendId, lastPagedActivityTime);
		
	}
	
	public void addToPagedFriendsCount(long userId, int updateCount) {
		
		Integer currentCount = pagedFriendsCounts.get(userId);
		currentCount = currentCount == null ? 0 : currentCount;
		pagedFriendsCounts.put(userId, currentCount + updateCount);
		
	}
	
	public void addToPagedActivitiesCount(long userId, int updateCount) {
		
		Integer currentCount = pagedActivitiesCounts.get(userId);
		currentCount = currentCount == null ? 0 : currentCount;
		pagedActivitiesCounts.put(userId, currentCount + updateCount);
		
	}
	
	public void resetPagedFriendsCount(long userId) {
		
		pagedFriendsCounts.put(userId, 0);
		lastPagedFriendsIds.put(userId, 0l);
		
	}
	
	public void resetPagedActivities(long userId) {
		
		pagedActivitiesCounts.put(userId, 0);
		lastPagedActivitiesTimes.put(userId, null);
		
	}
	
	public long getLastPagedFriendId(long userId) {
		
		Long result = lastPagedFriendsIds.get(userId);		
		return result == null ? 0l : result.longValue();
		
	}
	
	public long getLastPagedActivityTime(long userId, long friendId) {
		
		Map<Long, Long> friendsPaginationState = lastPagedActivitiesTimes.get(userId);
				
		if (friendsPaginationState == null) {
			return Long.MAX_VALUE;
		} else {
			Long result = friendsPaginationState.get(friendId); 
			return result == null ? Long.MAX_VALUE : result.longValue();
		}
		
	}
	
	public int getPagedFriendsCount(long userId) {
		
		Integer result = pagedFriendsCounts.get(userId); 
		return result == null ? 0 : result.intValue();
		
	}
	
	public int getPagedActivitiesCount(long userId) {
		
		Integer result =  pagedActivitiesCounts.get(userId); 
		return result == null ? 0 : result.intValue();
		
	}

}