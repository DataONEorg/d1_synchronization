package org.dataone.cn.batch.synchronization.tasks;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
/**
 * A wrapper class for objects that implements Delayed.
 * Credit for the wrappedObject code to Andriy Redko.  (Andriy Redko's {devmind} blog)
 * @see http://aredko.blogspot.com/2012/04/using-delayed-queues-in-practice.html
 * 
 * @author (Andriy Redko), rnahf
 *
 * @param <T> - the type of object being wrapped
 */
public class DelayWrapper<T> implements Delayed {


    private final long origin;
    private final long delay;
    private final T wrappedObject;

    public DelayWrapper( final T wrappedObject ) {
        this(wrappedObject, 0);
    }
    
    public DelayWrapper( final T queueItem, final long delay ) {
        if (queueItem == null) 
            throw new IllegalArgumentException("queue item parameter cannot be null"); 
        this.origin = System.currentTimeMillis();
        this.wrappedObject = queueItem;
        this.delay = delay;
    }

    public T getWrappedObject() {
        return this.wrappedObject;
    }
    
    @Override
    public long getDelay( TimeUnit unit ) {
        return unit.convert( delay - ( System.currentTimeMillis() - origin ), 
                TimeUnit.MILLISECONDS );
    }

    @Override
    public int compareTo( Delayed delayed ) {
        if( delayed == this ) {
            return 0;
        }

        if( delayed instanceof DelayWrapper<?> ) {
            long diff = delay - ( ( DelayWrapper<?> )delayed ).delay;
            return ( ( diff == 0 ) ? 0 : ( ( diff < 0 ) ? -1 : 1 ) );
        }

        long d = ( getDelay( TimeUnit.MILLISECONDS ) - delayed.getDelay( TimeUnit.MILLISECONDS ) );
        return ( ( d == 0 ) ? 0 : ( ( d < 0 ) ? -1 : 1 ) );
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;
        result = prime * result + ( ( this.wrappedObject == null ) ? 0 :this.wrappedObject.hashCode() );

        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if( this == obj ) {
            return true;
        }

        if( obj == null ) {
            return false;
        }

        if( !( obj instanceof DelayWrapper ) ) {
            return false;
        }

        final DelayWrapper<?> other = ( DelayWrapper<?> )obj;
        if( this.wrappedObject == null ) {
            if( other.wrappedObject != null ) {
                return false;
            }
        } else if( !this.wrappedObject.equals( other.wrappedObject ) ) {
            return false;
        }

        return true;
    }
}