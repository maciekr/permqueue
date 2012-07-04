permqueue
=========

Simple in-mem queue with disk backup based on Sleepycat

Usage:

```
int cacheFlushThreshold = 1000;
final PersistentQueue queue = new BDBJEPersistentQueue("queue", cacheFlushThreshold);
try {
	queue.add("something");
	String something = queue.poll();
}finally {
	boolean closeSleepycatEnv = false;
	queue.close(false);
}

see more in com.heyitworks.permqueue.PersistentQueueTest
