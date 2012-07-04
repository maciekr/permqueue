package com.heyitworks.permqueue;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author maciekr
 */
public abstract class PersistentQueueMBeanSupport {

    private static final Logger LOGGER = Logger.getLogger(PersistentQueueMBeanSupport.class.getCanonicalName());
    protected String queueName;
    private MBeanServer mBeanServer;

    public PersistentQueueMBeanSupport(String queueName) {
        this.queueName = queueName;
        initJmx();
    }

    private final void initJmx() {
        try {
            this.mBeanServer = ManagementFactory
                    .getPlatformMBeanServer();

            mBeanServer.registerMBean(this, new ObjectName(
                    "com.heyitworks.permqueue.PersistentQueue:name=" + queueName));

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
    }

    protected final void unregister() {
        try {

            mBeanServer.unregisterMBean(new ObjectName(
                    "com.heyitworks.permqueue.PersistentQueue:name=" + queueName));

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
    }
}
