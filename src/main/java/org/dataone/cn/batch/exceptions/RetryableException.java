package org.dataone.cn.batch.exceptions;

/**
 * An exception to capture cases where an internal failure in synchronization
 * happened that warrants retrying the synchronization of the object in the near future.
 * 
 * @author rnahf
 *
 */
public class RetryableException extends Exception {
    
    private long delay = 0;
    /**
     * 
     */
    private static final long serialVersionUID = 5115935521722185220L;
    
    public RetryableException() {
        super();
    }
    
    public RetryableException(String message) {
        super(message);
    }
    
    public RetryableException(String message, Throwable t) {
        super(message,t);
    }
    
    public RetryableException(String message, Throwable t, long retryDelay) {
        this(message,t);
        this.delay = retryDelay;
    }
    
    public void setDelay(long milliseconds) {
        this.delay = milliseconds;
    }
    
    public long getDelay() {
        return this.delay;
    }
}
