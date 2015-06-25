package org.dataone.cn.batch.exceptions;

/**
 * An exception to capture cases where an internal failure in synchronization
 * happens.  It is distinguished from SynchronizationFailed in that the failure
 * is not reported back to the Member Node via MN.synchronizationFailed
 * 
 * @author rnahf
 *
 */
public class UnrecoverableException extends Exception {
    public UnrecoverableException(String message) {
        super(message);
    }
    public UnrecoverableException(String message, Throwable t) {
        super(message,t);
    }
}
