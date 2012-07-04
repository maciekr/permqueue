package com.heyitworks.permqueue;

import com.sleepycat.je.*;
import org.perf4j.StopWatch;
import org.perf4j.javalog.JavaLogStopWatch;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author maciekr
 */
public class BDBJEPersistentQueue extends PersistentQueueMBeanSupport implements PersistentQueue, BDBJEPersistentQueueMBean {

    private static final Logger LOGGER = Logger.getLogger(BDBJEPersistentQueue.class.getCanonicalName());
    private static final long LOCK_TIMEOUT_MS = 2000;
    private static final long RETRY_DELAY_MS = 500;

    private int maxRetries = 3;
    private static Environment sharedDbEnvironment;

    private static final String storeDir = "queue-db-env";
    private Database myDatabase;
    private int operationsCount = 0;
    private int cacheFlushThreshold;
    private boolean persistenceOk = true;
    private Serializer serializer;

    private static class BigIntegerComparator implements Comparator<byte[]>, Serializable {
        public int compare(byte[] bytes, byte[] bytes1) {
            return new BigInteger(bytes).compareTo(new BigInteger(bytes1));
        }
    }

    private static final synchronized Environment getEnvironment() throws DatabaseException, IOException {
        if (sharedDbEnvironment == null) {
            String storeDirRoot = System.getProperty("com.heyitworks.permqueue.rootdir", ".");
            LOGGER.log(Level.INFO, "Initializing permqueue in " + new File(storeDirRoot).getCanonicalPath());
            new File(storeDirRoot, storeDir).mkdirs();
            // Open the environment. Create it if it does not already exist.
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setLockTimeout(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            envConfig.setAllowCreate(true);
            Environment environment = new Environment(new File(storeDir),
                    envConfig);
            sharedDbEnvironment = environment;
        }
        return sharedDbEnvironment;
    }

    public BDBJEPersistentQueue(final String queueName,
                                final int cacheFlushThreshold) throws IOException {
        this(queueName, cacheFlushThreshold, new KryoSerializer());
    }

    public BDBJEPersistentQueue(final String queueName,
                                final int cacheFlushThreshold, Serializer serializer) throws IOException {
        super(queueName);
        this.cacheFlushThreshold = cacheFlushThreshold;
        this.serializer = serializer;

        Environment environment = getEnvironment();

        // Open the database. Create it if it does not already exist.
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        dbConfig.setBtreeComparator(new BigIntegerComparator());
        myDatabase = environment.openDatabase(
                null,
                queueName,
                dbConfig);

    }

    public synchronized boolean add(Object data) {
        boolean result = true;
        long sleepMS = 0;
        for (int i = 1; i <= maxRetries; i++) {
            if (sleepMS != 0) {
                try {
                    Thread.sleep(sleepMS);
                } catch (InterruptedException e) {
                }
                sleepMS = 0;
            }

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            byte[] newDateByteArray = serialize(data);
            if (newDateByteArray != null) {

                StopWatch stopWatch = new JavaLogStopWatch("permqueue.ADD.inCursor");
                stopWatch.start();
                Cursor cursor = myDatabase.openCursor(null, null);
                try {
                    StopWatch lockAcquisitionStopWatch = new JavaLogStopWatch("permqueue.ADD.lockAcquire");
                    lockAcquisitionStopWatch.start();
                    cursor.getLast(key, value, LockMode.RMW);
                    lockAcquisitionStopWatch.stop();

                    BigInteger nextKey = getNextKey(key);
                    try {
                        final DatabaseEntry newKey = new DatabaseEntry(
                                nextKey.toByteArray());
                        final DatabaseEntry newData = new DatabaseEntry(newDateByteArray);
                        myDatabase.put(null, newKey, newData);

                        syncIfRequired();

                    } catch (Exception e) {
                        result = false;
                        LOGGER.log(Level.SEVERE, "Exception adding element to the queue: " + e.getMessage(), e);
                    }

                    break;

                } catch (LockConflictException lce) {
                    LOGGER.log(Level.SEVERE, "Lock exception adding element to the queue: " + lce.getMessage(), lce);
                    result = false;
                    sleepMS = RETRY_DELAY_MS;
                    continue;

                } finally {
                    cursor.close();
                    stopWatch.stop();
                }
            } else {
                result = false;
            }
        }
        return result;
    }

    public <T> T poll() {
        byte[] result = null;
        long sleepMS = 0;
        for (int i = 1; i <= maxRetries; i++) {
            if (sleepMS != 0) {
                try {
                    Thread.sleep(sleepMS);
                } catch (InterruptedException e) {
                }
                sleepMS = 0;
            }
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();

            StopWatch stopWatch = new JavaLogStopWatch("permqueue.POLL.inCursor");
            stopWatch.start();
            final Cursor cursor = myDatabase.openCursor(null, null);
            try {
                StopWatch lockAcquisitionStopWatch = new JavaLogStopWatch("permqueue.POLL.lockAcquire");
                lockAcquisitionStopWatch.start();
                cursor.getFirst(key, data, LockMode.RMW);
                lockAcquisitionStopWatch.stop();

                if (data.getData() != null) {
                    cursor.delete();
                    result = data.getData();
                    syncIfRequired();
                }

                break;

            } catch (LockConflictException lce) {
                LOGGER.log(Level.SEVERE, "Lock exception when adding element to the queue: " + lce.getMessage(), lce);
                sleepMS = RETRY_DELAY_MS;

                continue;

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Exception polling from the queue: " + e.getMessage(), e);
            } finally {
                cursor.close();
                stopWatch.stop();
            }
        }
        return result == null ? null : (T) deserialize(result);
    }

    public long size() {
        return myDatabase.count();
    }

    public <T> T peek() {
        byte[] result = null;
        long sleepMS = 0;
        for (int i = 1; i <= maxRetries; i++) {
            if (sleepMS != 0) {
                try {
                    Thread.sleep(sleepMS);
                } catch (InterruptedException e) {
                }
                sleepMS = 0;
            }
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();
            final Cursor cursor = myDatabase.openCursor(null, null);
            try {
                cursor.getFirst(key, data, LockMode.RMW);
                if (data.getData() != null) {
                    result = data.getData();
                }

                break;

            } catch (LockConflictException lce) {
                LOGGER.log(Level.SEVERE, "Lock exception adding element to the queue: " + lce.getMessage(), lce);
                sleepMS = RETRY_DELAY_MS;

                continue;

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Exception polling from the queue: " + e.getMessage(), e);
            } finally {
                cursor.close();
            }
        }
        return result == null ? null : (T) deserialize(result);
    }

    public synchronized void close(boolean closeEnv) {
        try {
            super.unregister();
            myDatabase.close();
            if (closeEnv && sharedDbEnvironment != null) {
                sharedDbEnvironment.close();
                sharedDbEnvironment = null;
            }
        } catch (Exception e) {
            //TODO: not sure what to do here, it's probably either already closed or in a really weird state. I'd probably just clear it?
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public boolean isPersistenceOk() {
        return persistenceOk;
    }

    /**
     * we would use integer sequence to keep stuff properly (fifo) ordered in the queue
     * this of course requires a custom key comparator as the default would use lexographical comaprisons...
     * <p/>
     * <p/>
     * this doesn't have to be synchornized as we are already in synchronized add() method
     *
     * @return
     */
    private BigInteger getNextKey(DatabaseEntry currentTipKey) {
        //use BigIntegers as they are easy to construct back from byte array
        BigInteger prev;
        if (currentTipKey.getData() == null) {
            prev = BigInteger.valueOf(-1);
        } else {
            prev = new BigInteger(currentTipKey.getData());
        }
        return prev.add(BigInteger.ONE);
    }

    private void syncIfRequired() {
        StopWatch stopWatch = new JavaLogStopWatch("permqueue.sync");
        stopWatch.start();
        if (++operationsCount >= cacheFlushThreshold) {
            myDatabase.sync();
            operationsCount = 0;
        }
        stopWatch.stop();
    }

    private byte[] serialize(Object data) {
        byte[] result = null;
        StopWatch stopWatch = new JavaLogStopWatch("permqueue.serialize");
        stopWatch.start();
        try {
            result = serializer.write(data);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Serialization failed: " + e.getMessage(), e);
        }
        stopWatch.stop();
        return result;
    }

    private Object deserialize(byte[] data) {
        Object result = null;
        StopWatch stopWatch = new JavaLogStopWatch("permqueue.deserialize");
        stopWatch.start();
        try {
            result = serializer.read(data);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Deserialization failed: " + e.getMessage(), e);
        }
        stopWatch.stop();
        return result;
    }

}
