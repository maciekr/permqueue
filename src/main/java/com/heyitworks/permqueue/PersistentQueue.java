package com.heyitworks.permqueue;

/**
 * @author maciekr
 */
public interface PersistentQueue {
    boolean add(Object o);
    <T> T poll();
    long size();
    void close(boolean closeEnv);
}
