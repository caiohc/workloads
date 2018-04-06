package workload.cassandra;

import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import util.RandomDataGenerator;
import util.Submitter;
import util.SubmitterException;

public class FowSaveActivity extends CassandraWorkload {
	
	private static final String CLUSTER = "cluster1";
	private static final String USERS_COUNT = "fow.usersCount";
	private static final String FRIENDS_PER_USER = "fow.friendsPerUser";
	private static final String ACTIVITIES_PER_USER = "fow.activitiesPerUser";
	
	private Session session;	
	private int usersCount;
	private int friendsPerUser;
	private int activitiesPerUser;
	private Calendar calendar;
	private AtomicLong keycounter;
	private RandomUserSelector userSelector;
	private PreparedStatement insertUserFriendStatement;
	private PreparedStatement insertActivityStatement;
	private PreparedStatement selectFriendsStatement;
	private PreparedStatement insertFeedStatement;
	
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
		keycounter = new AtomicLong(1);
		calendar = Calendar.getInstance();
		calendar.set(2015, 0, 1, 0, 0, 0);
		userSelector = new RandomUserSelector(usersCount, activitiesPerUser);
		
	}

	/**
	 * This method saves all the friends for a user, therefore the recordcount must be equal to the number of users. For instance,
	 * if there are 1000 users and each user has 100 friends, this method should execute 1000 times, because each time it will save
	 * the 100 friends of the current user. Therefore, the number of executions of this method, determined by the recordcount
	 * parameter, must be equal to the number of users.
	 */
	@Override
	public boolean doInsert(DB db, Object threadState) {
		
		BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
		long key = keycounter.getAndIncrement();
		
		for (int i = 0; i < friendsPerUser; i++) {
			batch.add(generateInsertUserFriendStatement(key, (key + i) % (usersCount) + 1));
		}		
		
		final long start = System.currentTimeMillis();		
		
		try {
			Submitter.submit(tries, maxBackoff, () -> session.execute(batch));
			Measurements.getMeasurements().measure("SAVE-FRIEND", (int) (System.currentTimeMillis() - start));
			return true;
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("SAVE-FRIEND-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());
			return false;
		} 		
		
	}
	
	/**
	 * Generates and saves one activity for a random user However, each time this method is executed, the activity is generated
	 * with a different creation time (plus one second).
	 */
	@Override
	public boolean doTransaction(DB db, Object threadState) {
		
		long dateTime = 0l;
		
		synchronized (calendar) {
			calendar.add(Calendar.SECOND, 1);
			dateTime = calendar.getTimeInMillis();
		}
		
		long userId = userSelector.getRandomUserId();
		
		String content = RandomDataGenerator.generateData(fieldSize);
		BoundStatement activityStatement = generateInsertUserActivityStatement(userId, dateTime, content);
		BoundStatement friendsStatement = generateSelectFriendsStatement(userId);
		long start = System.currentTimeMillis();		
		
		try {
			Submitter.submit(tries, maxBackoff, () -> session.execute(activityStatement));
			ResultSet friendsResultSet = (ResultSet) Submitter.submit(tries, maxBackoff, () -> session.execute(friendsStatement));
			List<Row> friendRows = friendsResultSet.all();
			
			for (Row friendRow : friendRows) {
				BoundStatement feedStatement = generateInsertFeedStatement(friendRow.getLong(1), dateTime, userId, content);
				Submitter.submit(tries, maxBackoff, () -> session.execute(feedStatement));
			}
			
			Measurements.getMeasurements().measure("SAVE-ACTIVITY", (int) (System.currentTimeMillis() - start));
		} catch (SubmitterException e) {
			Measurements.getMeasurements().measure("SAVE-ACTIVITY-FAILED", (int) (System.currentTimeMillis() - start));
			System.err.println(e.getMessage());			
		} 
		
		return true;
		
	}
	
	@Override
	public void prepareStatements() {
		
		StringBuilder insertUserFriendStatement = new StringBuilder();
		insertUserFriendStatement.append("INSERT INTO friend (user_id, friend_id) VALUES (?, ?)");
		this.insertUserFriendStatement = session.prepare(insertUserFriendStatement.toString());
		
		StringBuilder selectFriendsStatement = new StringBuilder();
		selectFriendsStatement.append("SELECT user_id, friend_id FROM friend WHERE user_id = ?");
		this.selectFriendsStatement = session.prepare(selectFriendsStatement.toString());
		
		StringBuilder insertActivityStatement = new StringBuilder();
		insertActivityStatement.append("INSERT INTO activity (user_id, created_at, content) VALUES (?, ?, ?)");
		this.insertActivityStatement = session.prepare(insertActivityStatement.toString());
		
		StringBuilder insertFeedStatement = new StringBuilder();
		insertFeedStatement.append("INSERT INTO feed (user_id, created_at, friend_id, content) VALUES (?, ?, ?, ?)");
		this.insertFeedStatement = session.prepare(insertFeedStatement.toString());

	}
	
	private BoundStatement generateInsertUserFriendStatement(long userId, long friendId) {
		
		return insertUserFriendStatement.bind(Utils.hash(userId), Utils.hash(friendId));
				
	}

	private BoundStatement generateSelectFriendsStatement(long userId) {
		
		return selectFriendsStatement.bind(Utils.hash(userId));
				
	}
	
	private BoundStatement generateInsertUserActivityStatement(long userId, long date, String content) {
		
		return insertActivityStatement.bind(Utils.hash(userId), date, content);
				
	}
	
	private BoundStatement generateInsertFeedStatement(long userId, long date, long friendId, String content) {
		
		return insertFeedStatement.bind(userId, date, Utils.hash(friendId), content);
				
	}

}	