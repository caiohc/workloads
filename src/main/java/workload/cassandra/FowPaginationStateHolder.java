package workload.cassandra;

import java.util.Map;

public class FowPaginationStateHolder {
	
	Map<String, Long>[] paginationStates;
	private boolean[] userLocks;
	
	@SuppressWarnings("unchecked")
	public FowPaginationStateHolder(int userCount) {
		
		paginationStates = new Map[userCount + 1];
		userLocks = new boolean[userCount + 1]; //Index 0 is not used.
		
	}
	
	public Map<String, Long> getPaginationState(int userId) {
		
		return paginationStates[userId]; 
		
	}
	
	public void resetPaginationState(int userId) {
		
		paginationStates[userId] = null;
		
	}
	
	public void setPaginationState(int userId, Map<String, Long> paginationState) {

		paginationStates[userId] = paginationState;
		
	}
	
	public boolean lockUser(int userId) { 
		
		if (userLocks[userId]) { //If the user is already locked...
			return false; //...refuse locking request.
		} else { //If the user is not locked...
			userLocks[userId] = true; //...lock user...
			return true; //...confirm lock request.
		}
		
	}
	
	public void unlockUser(int userId) { userLocks[userId] = false; }

}