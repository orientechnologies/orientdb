package com.orientechnologies.orient.test.internal.lock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.concur.lock.OLockManager.LOCK;

public class LockManagerTest {

	public static final int														THREADS					= 100;
	public static int																	cyclesByProcess	= 1000;
	public static boolean															verbose					= false;
	public static OLockManager<Callable<?>, Runnable>	lockMgr					= new OLockManager<Callable<?>, Runnable>();
	protected List<Callable<?>>												resources				= new ArrayList<Callable<?>>();
	protected List<Thread>														processes				= Collections.synchronizedList(new ArrayList<Thread>());
	protected List<Throwable>													exceptions			= Collections.synchronizedList(new ArrayList<Throwable>());
	protected AtomicInteger														counter					= new AtomicInteger();

	public static class ResourceRead implements Callable<Void> {
		volatile int	countRead	= 0;

		public Void call() throws Exception {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.SHARED);
			try {
				countRead++;
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceRead locked by " + Thread.currentThread());
			} finally {
				countRead--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
			return null;
		}
	}

	public static class ResourceWrite implements Callable<Void> {
		volatile int	countWrite	= 0;

		public Void call() throws Exception {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countWrite++;
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceWrite locked by " + Thread.currentThread());
				if (countWrite != 1)
					throw new AssertionError("countWrite:" + countWrite);
			} finally {
				countWrite--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			}
			return null;
		}
	}

	public static class ResourceReadWrite implements Callable<Void> {
		volatile int			countRead		= 0;
		volatile int			countWrite	= 0;
		volatile boolean	lastWasRead;

		public Void call() throws Exception {
			if (lastWasRead) {
				write();
			} else {
				read();
			}
			lastWasRead = !lastWasRead;
			return null;
		}

		void read() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.SHARED);
			try {
				countRead++;
				if (verbose)
					System.out.println("ResourceReadWrite SHARED locked by " + Thread.currentThread());
			} catch (RuntimeException e) {
				e.printStackTrace();
				throw e;
			} finally {
				countRead--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
		}

		void write() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countWrite++;
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceReadWrite EXCLUSIVE locked by " + Thread.currentThread());
				if (countWrite != 1)
					throw new AssertionError("countWrite:" + countWrite);
				if (countRead != 0)
					throw new AssertionError("countRead:" + countRead);
			} finally {
				countWrite--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			}
		}
	}

	public static class ResourceReantrance implements Callable<Void> {

		volatile int	countRead						= 0;
		volatile int	countReentrantRead	= 0;
		volatile int	countWrite					= 0;
		volatile int	countReentrantWrite	= 0;

		public Void call() throws Exception {
			read();
			return null;
		}

		void read() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.SHARED);
			try {
				countRead++;
				// while (countRead < 3) {
				// // wait for 3 concurrent threads at this point.
				// Thread.yield();
				// }
				rentrantRead();
				write();
			} catch (RuntimeException e) {
				e.printStackTrace();
				throw e;
			} finally {
				countRead--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
		}

		void rentrantRead() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.SHARED);
			try {
				countReentrantRead++;
				// while (countRead < 2) {
				// // wait an other thread.
				// Thread.yield();
				// }
				write();
			} finally {
				countReentrantRead--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
		}

		void write() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countWrite++;
				reentrantWrite();
				// for (int i = 0; i < 10000000; i++) {
				// }
				// if(log) System.out.println("ResourceReantrance locked by " + Thread.currentThread());
				if (countWrite != 1)
					throw new AssertionError("countWrite:" + countWrite);
			} finally {
				countWrite--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			}
		}

		void reentrantWrite() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countReentrantWrite++;
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceReantrance locked by " + Thread.currentThread());
				if (countReentrantWrite != 1)
					throw new AssertionError("countReentrantWrite:" + countReentrantWrite);
			} finally {
				countReentrantWrite--;
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			}
		}
	}

	public class Process implements Runnable {

		public void run() {
			try {
				for (int i = 0; i < cyclesByProcess; i++) {
					for (Callable<?> res : resources) {
						if (exceptions.size() > 0)
							return;
						res.call();
						counter.incrementAndGet();
					}
				}

			} catch (Throwable e) {
				e.printStackTrace();
				exceptions.add(e);
			}
		}
	}

	@Test
	public void testConcurrentAccess() throws Throwable {

		resources.add(new ResourceRead());
		resources.add(new ResourceWrite());
		resources.add(new ResourceReadWrite());
		resources.add(new ResourceReantrance());

		System.out.println("Starting " + THREADS + " threads: ");

		for (int i = 0; i < THREADS; ++i) {
			if (i % THREADS / 10 == 0)
				System.out.print('.');

			processes.add(new Thread(new Process()));
		}

		for (Thread thread : processes) {
			thread.start();
		}

		System.out.println("\nOk, waiting for end: ");

		for (int i = 0; i < THREADS; ++i) {
			if (i % THREADS / 10 == 0)
				System.out.print('.');

			processes.get(i).join();
		}

		System.out.println("\nOk, all threads back");

		// Pulish exceptions.
		if (exceptions.size() > 0) {
			for (Throwable exc : exceptions) {
				exc.printStackTrace();
			}
			throw exceptions.get(0);
		}

		Assert.assertEquals(counter.get(), processes.size() * resources.size() * cyclesByProcess);
	}
}
