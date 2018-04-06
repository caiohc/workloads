package workload.cassandra;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.Submitter;
import util.SubmitterException;

public class ForPaginateFeeds extends CassandraWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String USERS_COUNT = "for.usersCount";
	private static final String FRIENDS_PER_USER = "for.friendsPerUser";
	private static final String FRIENDS_PER_PAGE = "for.friendsPerPage";
	private static final String ACTIVITIES_PER_USER = "for.activitiesPerUser";
	private static final String FRIEND_ACTIVITIES_PER_PAGE = "for.friendActivitiesPerPage";
	
	private Session session;	
	private int usersCount;
	private int friendsPerUser;
	private int friendsPerPage;	
	private int activitiesPerUser;
	private int friendActivitiesPerPage;
	private AtomicLong userCounter;
	private ForPaginationStateHolder paginator;
	private PreparedStatement queryFriendsStatement;
	private PreparedStatement queryActivitiesStatement;
	
	@Override
	public void init(final Properties properties) {
	
		try {
			super.init(properties);
			session = connectionManager.getSession(CLUSTER);	
			prepareStatements();
		} catch (WorkloadException e) {
			System.err.printf("Error on startup: \n%s\n", e.getMessage());
			System.exit(1);
		}
		
		usersCount = Integer.parseInt(properties.getProperty(USERS_COUNT, "100"));
		friendsPerUser = Integer.parseInt(properties.getProperty(FRIENDS_PER_USER, "10"));
		friendsPerPage = Integer.parseInt(properties.getProperty(FRIENDS_PER_PAGE, "10"));
		activitiesPerUser = Integer.parseInt(properties.getProperty(ACTIVITIES_PER_USER, "10"));
		friendActivitiesPerPage = Integer.parseInt(properties.getProperty(FRIEND_ACTIVITIES_PER_PAGE, "1"));
		userCounter = new AtomicLong(0);
		paginator = new ForPaginationStateHolder(usersCount, friendsPerUser);
		
	}
	
	@Override
	public boolean doTransaction(DB db, Object threadState) {
		
		long userId = 0;
		boolean userLocked = false;		
		
		do {
			userId = (userCounter.getAndIncrement() % usersCount) + 1; //Gets next user.			
			synchronized (this) { //Only one thread at a time can request a user lock.
				userLocked = paginator.lockUser((int) userId); //Tries to lock the user for the current thread.
			}			
		} while (!userLocked); //Checks if the user was successfully locked.
				
		long lastPagedFriendId = paginator.getLastPagedFriendId(userId);
		
		if (paginator.getPagedFriendsCount(userId) == friendsPerUser) { //Checks if all friends of the user were paginated.
			paginator.resetPagedFriendsCount(userId); //Resets friends pagination.
			lastPagedFriendId = 0;
			//Updates number of activities paged. When it reaches the total of activities, pagination must be restarted.
			paginator.addToPagedActivitiesCount(userId, friendActivitiesPerPage);			
			
			if (paginator.getPagedActivitiesCount(userId) == activitiesPerUser) { //Checks if all friends activities were paginated. 
				paginator.resetPagedActivities(userId); //Resets friends activities pagination.
			}
			
		}
		
		BoundStatement friendsStatement = generateQueryFriendsStatement(userId, lastPagedFriendId, friendsPerPage);
		long start = System.currentTimeMillis();		
		
		try {
			//Queries user friends.
			List<Row> friendsRecords = 
					((ResultSet) Submitter.submit(tries, maxBackoff, () -> session.execute(friendsStatement))).all();
			int activitiesResultCount = 0;
			
			for (Row friendRecord : friendsRecords) { //Queries activities of each friend.
				long friendId = friendRecord.getLong(1);
				long lastPagedActivityTime = paginator.getLastPagedActivityTime(userId, friendId);
				BoundStatement activitiesStatement = 
						generateQueryActivitiesStatement(friendId, lastPagedActivityTime, friendActivitiesPerPage);
				List<Row> activitiesRecords = 
						((ResultSet) Submitter.submit(tries, maxBackoff, () -> session.execute(activitiesStatement))).all();
				activitiesResultCount = activitiesRecords.size();
				//Saves last activity paged for the current friend.
				paginator.setLastPagedActivityTime(userId, friendId, activitiesRecords.get(activitiesResultCount - 1).getLong(1));	
				
				/*for (Row activityRecord : activitiesRecords) {
					System.out.printf("User: %d\tFriend: %d\tActivity time: %d\n", userId, friendId, activityRecord.getLong(1));
				}*/
				
			}			
			
			Measurements.getMeasurements().measure("PAGINATE-FEED", (int) (System.currentTimeMillis() - start));
			int friendsResultCount = friendsRecords.size();
			//Saves last friend paged for the current user.
			paginator.setLastPagedFriendId(userId, friendsRecords.get(friendsResultCount - 1).getLong(1));
			//Updates number of friends paged. When it reaches the total of user friends, pagination must be restarted.
			paginator.addToPagedFriendsCount(userId, friendsResultCount);			
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("PAGINATE-FEED-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		} 
		
		paginator.unlockUser((int) userId); //Unlock the user so other thread can paginate is friends activities.		
		return true;
		
	}
	
	@Override
	public void prepareStatements() {
		
		final StringBuilder queryFriendsStatement = new StringBuilder();
		queryFriendsStatement.append("SELECT user_id, friend_id FROM friend WHERE user_id = ? AND friend_id  > ? LIMIT ?");
		this.queryFriendsStatement = session.prepare(queryFriendsStatement.toString());
		
		final StringBuilder queryActivitiesStatement = new StringBuilder();
		queryActivitiesStatement.append("SELECT user_id, created_at, content FROM activity WHERE user_id = ? AND created_at  < ? LIMIT ?");
		this.queryActivitiesStatement = session.prepare(queryActivitiesStatement.toString());

	}
	
	private BoundStatement generateQueryFriendsStatement(long userId, long lastPagedFriendId, int friendsPerPage) {
		
		return queryFriendsStatement.bind(Utils.hash(userId),
				lastPagedFriendId == 0 ? 0 : lastPagedFriendId, friendsPerPage);
				
	}
	
	private BoundStatement generateQueryActivitiesStatement(long userId, long createdAt, int friendActivitesPerPage) {
		
		return queryActivitiesStatement.bind(userId, createdAt, friendActivitesPerPage);
				
	}

}	