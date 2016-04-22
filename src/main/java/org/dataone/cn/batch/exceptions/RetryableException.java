package org.dataone.cn.batch.exceptions;

/**
 * An exception to capture cases where an internal failure in synchronization
 * happens.  It is distinguished from SynchronizationFailed in that the failure
 * is not reported back to the Member Node via MN.synchronizationFailed
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
