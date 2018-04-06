package workload.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class FowPaginateFeeds extends CassandraWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String USERS_COUNT = "fow.usersCount";
	private static final String FRIENDS_PER_USER = "fow.friendsPerUser";
	private static final String ACTIVITIES_PER_USER = "fow.activitiesPerUser";
	private static final String ACTIVITIES_PER_PAGE = "fow.activitiesPerPage";
	
	private Session session;	
	private int usersCount;
	private int friendsPerUser;
	private int activitiesPerUser;
	private int activitiesPerPage;
	private AtomicLong userCounter;
	private FowPaginationStateHolder paginator;
	private PreparedStatement queryFeedStatement;
	private int pagesPerUser;
	
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
		activitiesPerUser = Integer.parseInt(properties.getProperty(ACTIVITIES_PER_USER, "10"));
		activitiesPerPage = Integer.parseInt(properties.getProperty(ACTIVITIES_PER_PAGE, "10"));
		userCounter = new AtomicLong(0);
		paginator = new FowPaginationStateHolder(usersCount);
		pagesPerUser = friendsPerUser * activitiesPerUser / activitiesPerPage;
		
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
		
		Map<String, Long> paginationState = paginator.getPaginationState((int) userId);
		long lastPaginatedActivityTime = Long.MAX_VALUE;
		long pageCount = 0;
		
		if (paginationState != null) {
			lastPaginatedActivityTime = paginationState.get("activityTime");
			pageCount = paginationState.get("pageCount");
		}
		
		BoundStatement feedStatement = generateQueryFeedStatement(userId, lastPaginatedActivityTime, activitiesPerPage);
		long start = System.currentTimeMillis();
		
		try {
			List<Row> activityRecords = 
					((ResultSet) Submitter.submit(tries, maxBackoff, () -> session.execute(feedStatement))).all();
			
			/*for (Row activityRecord : activityRecords) {
				System.out.printf("User: %d\tActivity time: %d\tFriend: %d\n", activityRecord.getLong(0),
						activityRecord.getLong(1), activityRecord.getLong(2));
			}*/
			
			if (++pageCount == pagesPerUser) {
				paginator.resetPaginationState((int) userId); 
			} else {
				Row lastRecord = activityRecords.get(activityRecords.size() - 1);
				paginationState = new HashMap<String, Long>(3);	
				paginationState.put("activityTime", lastRecord.getLong(1));
				paginationState.put("pageCount", pageCount);
				paginator.setPaginationState((int) userId, paginationState);				
			}
			
			Measurements.getMeasurements().measure("PAGINATE-FEED", (int) (System.currentTimeMillis() - start));			
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("PAGINATE-FEED-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		}
		
		paginator.unlockUser((int) userId); //Unlock the user so other thread can paginate is friends activities.		
		return true;
		
	}
	
	@Override
	public void prepareStatements() {
		
		final StringBuilder queryFeedStatement = new StringBuilder();
		queryFeedStatement.append("SELECT user_id, created_at, friend_id, content FROM feed ");
		queryFeedStatement.append("WHERE user_id = ? AND created_at  < ? LIMIT ?");
		this.queryFeedStatement = session.prepare(queryFeedStatement.toString());

	}
	
	private BoundStatement generateQueryFeedStatement(long userId, long createdAt, int activitiesPerPage) {
		
		return queryFeedStatement.bind(Utils.hash(userId), createdAt, activitiesPerPage);
				
	}

}