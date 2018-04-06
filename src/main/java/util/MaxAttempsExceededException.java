package util;

@SuppressWarnings("serial")
public class MaxAttempsExceededException extends SubmitterException { 
	
	public MaxAttempsExceededException() {
		
		super("Exceeded maximum number of attempts");
		
	}
	
}
