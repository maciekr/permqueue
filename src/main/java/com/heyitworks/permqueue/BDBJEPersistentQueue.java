package com.heyitworks.permqueue;

import com.sleepycat.je.*;

import java.io.*;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author maciekr
 */
public class BDBJEPersistentQueue implements PersistentQueue {

    private static final Logger LOGGER = Logger.getLogger(BDBJEPersistentQueue.class.getCanonicalName());

    private static Environment sharedDbEnvironment;
    private static final String storeDir = "queue-db-env";

    private Database myDatabase;
    private int operationsCount = 0;
    private int cacheFlushThreshold;
    private boolean persistenceOk = true;
    private Serializer serializer;

    private static class BigIntegerComparator implements Comparator<byte[]>, Serializable {
        @Override
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

    @Override
    public synchronized boolean add(Object data) {
        boolean result = true;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        Cursor cursor = myDatabase.openCursor(null, null);
        try {
            cursor.getLast(key, value, LockMode.RMW);

            BigInteger nextKey = getNextKey(key);
            try {
                final DatabaseEntry newKey = new DatabaseEntry(
                        nextKey.toByteArray());
                final DatabaseEntry newData = new DatabaseEntry(serialize(data));
                myDatabase.put(null, newKey, newData);

                syncIfRequired();

            } catch (Exception e) {
                //TODO: test if this holds when duplicate key inserted
                result = false;
                LOGGER.log(Level.SEVERE, "Exception adding element to the queue: " + e.getMessage(), e);
            }
        } finally {
            cursor.close();
        }
        return result;
    }

    @Override
    public <T> T poll() {
        T result = null;
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = myDatabase.openCursor(null, null);
        try {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() != null) {
                result = (T) deserialize(data.getData());
                cursor.delete();
                syncIfRequired();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception polling from the queue: " + e.getMessage(), e);
        } finally {
            cursor.close();
        }
        return result;
    }

    @Override
    public long size() {
        return myDatabase.count();
    }

    @Override
    public synchronized void close(boolean closeEnv) {
        try {
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
        if (++operationsCount >= cacheFlushThreshold) {
            myDatabase.sync();
            operationsCount = 0;
        }
    }

    private byte[] serialize(Object data) throws Exception {
        return serializer.write(data);
    }

    private Object deserialize(byte[] data) throws Exception {
        return serializer.read(data);
    }

}
