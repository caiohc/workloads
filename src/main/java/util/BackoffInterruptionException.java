package util;

@SuppressWarnings("serial")
public class BackoffInterruptionException extends SubmitterException { 
	
	public BackoffInterruptionException() {
	
		super("Thread backoff sleep was interrupted.");
		
	}
	
}
