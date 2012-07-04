package com.heyitworks.permqueue;

/**
@author maciekr
 */
public interface BDBJEPersistentQueueMBean {

    long size();
    <T> T peek();

}
