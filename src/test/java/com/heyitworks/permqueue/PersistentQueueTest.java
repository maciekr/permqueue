package com.heyitworks.permqueue;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author maciekr
 */
public class PersistentQueueTest {

    @Test
    public void testConfig() throws IOException {
        BDBJEPersistentQueue queueA = new BDBJEPersistentQueue("queueA", 10);
        try {
            assertNotNull(queueA);
            assertTrue(queueA.isPersistenceOk());
        } finally {
            queueA.close(true);
        }
    }

    @Test
    public void testReopenDb() throws IOException {
        PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 10);
        drainQueue(queueA);
        try {
            queueA.add(new String("test1"));
        } finally {
            queueA.close(false);
        }

        queueA = new BDBJEPersistentQueue("queueA", 10);
        try {
            assertEquals("test1", queueA.poll());
        } finally {
            queueA.close(true);
        }
    }

    @Test
    public void testReopenEnv() throws IOException {

        PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 10);
        try {
            drainQueue(queueA);
            queueA.add(new String("test1"));
        } finally {
            queueA.close(true);
            queueA.close(true);//noop
        }

        queueA = new BDBJEPersistentQueue("queueA", 10);
        try {
            queueA.add(new String("test2"));
            assertEquals("test1", queueA.poll());
            assertEquals("test2", queueA.poll());
        } finally {
            queueA.close(true);
        }
    }

    @Test
    public void testReopenEnv_Drain() throws IOException {

        PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 10);
        try {
            queueA.add(new String("test1"));
        } finally {
            queueA.close(true);
        }

        queueA = new BDBJEPersistentQueue("queueA", 10);
        try {
            drainQueue(queueA);
            assertNull(queueA.poll());
        } finally {
            queueA.close(true);
        }
    }

    @Test
    public void addSomeAndTrySteppingThrough() throws IOException {
        PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 100);
        try {
            drainQueue(queueA);
            String[] data = new String[1000];
            for (int i = 0; i < data.length; i++) {
                data[i] = UUID.randomUUID().toString();
                queueA.add(data[i]);
            }
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], queueA.poll());
            }
            assertEquals(0L, queueA.size());
        } finally {
            queueA.close(true);
        }
    }

    @Test
    public void testMultiThreadedPollYieldsNoDups() throws Throwable {
        final PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 100);
        try {
            int threadCount = 50;
            for (int i = 0; i < threadCount; i++)
                queueA.add(String.valueOf(i));

            final Set setOfConsumedElements = Collections.synchronizedSet(new HashSet<Object>());
            final CountDownLatch latch = new CountDownLatch(threadCount);
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);

            for (int i = 0; i < threadCount; i++) {
                new Thread() {
                    public void run() {
                        try {
                            barrier.await();
                            String val = queueA.poll();
                            if (val != null) {
                                setOfConsumedElements.add(val);
                            }
                            latch.countDown();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            }

            latch.await(5, TimeUnit.SECONDS);

            assertEquals(threadCount, setOfConsumedElements.size());

        } finally {
            queueA.close(true);
        }
    }

    @Test
    public void testMultiThreadedAdd() throws IOException, InterruptedException {
        final PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 100);
        try {
            int threadCount = 50;

            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                new Thread(String.valueOf(i)) {
                    public void run() {
                        try {
                            barrier.await();

                            queueA.add(getName());
                            latch.countDown();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            }

            latch.await(5, TimeUnit.SECONDS);

            assertEquals(1l * threadCount, queueA.size());

        } finally {
            queueA.close(true);
        }
    }

    @Test
    public void testMultipleQueuesInOneEnv() throws IOException {
        final PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 100);
        final PersistentQueue queueB = new BDBJEPersistentQueue("queueB", 100);
        final PersistentQueue queueC = new BDBJEPersistentQueue("queueC", 100);
        try {
            drainQueue(queueA);
            drainQueue(queueB);
            drainQueue(queueC);
            int totalElCount = 300;

            for (int i = 0; i < totalElCount; i++) {
                switch (i % 3) {
                    case 0:
                        queueA.add(i);
                        break;
                    case 1:
                        queueB.add(i);
                        break;
                    case 2:
                        queueC.add(i);
                        break;
                }
            }

            for (int i = 0; i < totalElCount; i++) {
                PersistentQueue q = null;
                switch (i % 3) {
                    case 0:
                        q = queueA;
                        break;
                    case 1:
                        q = queueB;
                        break;
                    case 2:
                        q = queueC;
                        break;
                }
                Integer el = q.poll();
                assertEquals(i % 3, el % 3);
            }
        } finally {
            queueA.close(false);
            queueB.close(false);
            queueC.close(true);
        }
    }

    @Test
    public void notReallyATest_Throughput() throws IOException {

        PersistentQueue queueA = new BDBJEPersistentQueue("queueA", 1000);

        try {
            drainQueue(queueA);
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                queueA.add(i);
                assertEquals(i, queueA.poll());
            }
            long t2 = System.currentTimeMillis();
            System.out.println(t2 - t1);
        } finally {
            queueA.close(true);
        }
    }

    private void drainQueue(PersistentQueue queue) {
        while (queue.poll() != null) ;
    }


}
