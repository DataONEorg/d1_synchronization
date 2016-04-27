package org.dataone.cn.batch.exceptions;

/**
 * An exception to capture cases where an internal failure in synchronization
 * happens that was not due to issues with Member Node content.  It is a wrapper 
 * exception whose use is primarily within (V2)TransferObjectTask.
 * 
 * @author rnahf
 *
 */
public class UnrecoverableException extends Exception {
    /**
     * 
     */
    private static final long serialVersionUID = 4982558294924192452L;
    
    public UnrecoverableException(String message) {
        super(message);
    }
    public UnrecoverableException(String message, Throwable t) {
        super(message,t);
    }
}
