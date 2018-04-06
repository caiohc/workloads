package workload;

import java.util.Calendar;
import java.util.Properties;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;

public abstract class PatternWorkload extends Workload {
	
	private static final String THREADCOUNT = "threadcount";
	private static final String TRIES = "tries";
	private static final String FIELD_SIZE = "fieldSize";
	private static final String MAX_BACKOFF = "maxBackoff";
	private static final String WAITING_TIME = "waitingTime";
	
	protected int tries;
	protected int threadCount;
	protected int fieldSize;
	protected int maxBackoff;
	protected int waitingTime;
	
	@Override
	public void init(final Properties properties) throws WorkloadException {
		
		super.init(properties);	
		
		tries = Integer.parseInt(properties.getProperty(TRIES, "10"));
		threadCount = Integer.parseInt(properties.getProperty(THREADCOUNT, "1"));
		fieldSize = Integer.parseInt(properties.getProperty(FIELD_SIZE, "25"));
		maxBackoff = Integer.parseInt(properties.getProperty(MAX_BACKOFF, "1000"));
		
		final String waitingTime = properties.getProperty(WAITING_TIME);
		
		if (waitingTime != null) {
			this.waitingTime = Integer.parseInt(waitingTime); 
			Calendar startTime = Calendar.getInstance();
			startTime.set(Calendar.MINUTE, startTime.get(Calendar.MINUTE) + this.waitingTime);
			startTime.set(Calendar.SECOND, 0);
			startTime.set(Calendar.MILLISECOND, 0);
			long waitInterval = startTime.getTimeInMillis() - System.currentTimeMillis();
			System.out.printf("Workload execution should start at: %tT\n", startTime.getTime());
		
			try {
				Thread.sleep(waitInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new WorkloadException("An error occurred during the waiting time.");
			}
		
		}
		
	}
	
	@Override
	public boolean doInsert(final DB db, final Object threadState) { return false; }
	
	@Override
	public boolean doTransaction(final DB db, final Object threadState) { return false; }

}