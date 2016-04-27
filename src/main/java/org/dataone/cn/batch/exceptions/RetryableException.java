package org.dataone.cn.batch.exceptions;

/**
 * An exception to capture cases where an internal failure in synchronization
 * happened that warrants retrying the synchronization of the object in the near future.
 * 
 * @author rnahf
 *
 */
public class RetryableException extends Exception {
    /**
     * 
     */
    private static final long serialVersionUID = 5115935521722185220L;
    
    public RetryableException(String message) {
        super(message);
    }
    public RetryableException(String message, Throwable t) {
        super(message,t);
    }
}
