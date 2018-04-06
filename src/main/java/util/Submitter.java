package util;

public class Submitter {
	
	public static Object submit(final int tries, final int maxBackoff, final Request request,
			final boolean throwsOnError, @SuppressWarnings("rawtypes") Class... exceptions)
			throws BackoffInterruptionException, MaxAttempsExceededException {
		
		int backoffExp = 0;
		while (backoffExp < tries) {
			
			try {
				return request.execute();			
			} catch (RuntimeException re) {
				
				if (throwsOnError) {
					
					for (@SuppressWarnings("rawtypes") Class exception : exceptions) {
						
						if (exception.isInstance(re)) {
							throw re;
						}
							
					}
					
				}
				
				if (backoffExp > 0) {
					
					try {
						Thread.sleep((int) Math.min(maxBackoff, Math.pow(2, backoffExp)));												
					} catch (InterruptedException ie) {
						throw new BackoffInterruptionException();
					}
			        
				}
				
				backoffExp++;
			}
			
		}
		
		throw new MaxAttempsExceededException();
		
	}
	
	public static void submit(final int tries, final int maxBackoff, final VoidRequest request,
			final boolean throwsOnError, @SuppressWarnings("rawtypes") Class... exceptions)
			throws BackoffInterruptionException, MaxAttempsExceededException {
		
		int backoffExp = 0;
		
		while (backoffExp < tries) {
			
			try {
				request.execute();	
				return;
			} catch (RuntimeException re) {
				
				if (throwsOnError) {
					
					for (@SuppressWarnings("rawtypes") Class exception : exceptions) {
						
						if (exception.isInstance(re)) {
							throw re;
						}
							
					}
					
				}
				
				if (backoffExp > 0) {
					
					try {
						Thread.sleep((int) Math.min(maxBackoff, Math.pow(2, backoffExp)));												
					} catch (InterruptedException ie) {
						throw new BackoffInterruptionException();
					}
			        
				}
			        
				backoffExp++;
			}
			
		}
		
		throw new MaxAttempsExceededException();
		
	}
	
	public static Object submit(final int tries, final int maxBackoff, final Request request)
			throws BackoffInterruptionException, MaxAttempsExceededException {
		
		return submit(tries, maxBackoff, request, false);
		
	}
	
	public static void submit(final int tries, final int maxBackoff, final VoidRequest request)
			throws BackoffInterruptionException, MaxAttempsExceededException {
		
		submit(tries, maxBackoff, request, false);
		
	}

}