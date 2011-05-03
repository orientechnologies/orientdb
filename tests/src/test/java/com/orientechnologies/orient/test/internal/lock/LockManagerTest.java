/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/**
 * Test class for OLockManager
 * 
 * @author Sylvain Spinelli
 * 
 */
public class LockManagerTest {

	public static final int														THREADS					= 100;
	public static int																	cyclesByProcess	= 10000;
	public static boolean															verbose					= false;
	public static OLockManager<Callable<?>, Runnable>	lockMgr					= new OLockManager<Callable<?>, Runnable>(5000);
	protected List<Callable<?>>												resources				= new ArrayList<Callable<?>>();
	protected List<Thread>														processes				= Collections.synchronizedList(new ArrayList<Thread>());
	protected List<Throwable>													exceptions			= Collections.synchronizedList(new ArrayList<Throwable>());
	protected AtomicInteger														counter					= new AtomicInteger();

	public static class ResourceRead implements Callable<Void> {
		AtomicInteger	countRead	= new AtomicInteger(0);

		public Void call() throws Exception {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.SHARED);
			try {
				countRead.incrementAndGet();
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceRead locked by " + Thread.currentThread());
			} finally {
				countRead.decrementAndGet();
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
			return null;
		}
	}

	public static class ResourceWrite implements Callable<Void> {
		AtomicInteger	countWrite	= new AtomicInteger(0);

		public Void call() throws Exception {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countWrite.incrementAndGet();
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceWrite locked by " + Thread.currentThread());
				if (countWrite.get() != 1)
					throw new AssertionError("countWrite:" + countWrite);
			} finally {
				countWrite.decrementAndGet();
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			}
			return null;
		}
	}

	public static class ResourceReadWrite implements Callable<Void> {
		AtomicInteger			countRead		= new AtomicInteger(0);
		AtomicInteger			countWrite	= new AtomicInteger(0);
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
				countRead.incrementAndGet();
				if (verbose)
					System.out.println("ResourceReadWrite SHARED locked by " + Thread.currentThread());
			} catch (RuntimeException e) {
				e.printStackTrace();
				throw e;
			} finally {
				countRead.decrementAndGet();
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
		}

		void write() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countWrite.incrementAndGet();
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceReadWrite EXCLUSIVE locked by " + Thread.currentThread());
				if (countWrite.get() != 1)
					throw new AssertionError("countWrite:" + countWrite);
				if (countRead.get() != 0)
					throw new AssertionError("countRead:" + countRead);
			} finally {
				countWrite.decrementAndGet();
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			}
		}
	}

	public static class ResourceReantrance implements Callable<Void> {

		AtomicInteger	countRead						= new AtomicInteger(0);
		AtomicInteger	countReentrantRead	= new AtomicInteger(0);
		AtomicInteger	countWrite					= new AtomicInteger(0);
		AtomicInteger	countReentrantWrite	= new AtomicInteger(0);

		public Void call() throws Exception {
			write();
			return null;
		}

		void read() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.SHARED);
			try {
				countRead.incrementAndGet();
				// while (countRead < 3) {
				// // wait for 3 concurrent threads at this point.
				// Thread.yield();
				// }
				reentrantRead();
			} catch (RuntimeException e) {
				e.printStackTrace();
				throw e;
			} finally {
				countRead.decrementAndGet();
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
		}

		void reentrantRead() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.SHARED);
			try {
				countReentrantRead.incrementAndGet();
				// while (countRead < 2) {
				// // wait an other thread.
				// Thread.yield();
				// }
				// write();
			} finally {
				countReentrantRead.decrementAndGet();
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.SHARED);
			}
		}

		void write() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countWrite.incrementAndGet();
				reentrantWrite();
				// for (int i = 0; i < 10000000; i++) {
				// }
				// if(log) System.out.println("ResourceReantrance locked by " + Thread.currentThread());
				if (countWrite.get() != 1)
					throw new AssertionError("countWrite:" + countWrite);
			} finally {
				countWrite.decrementAndGet();
				lockMgr.releaseLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			}
		}

		void reentrantWrite() {
			lockMgr.acquireLock(Thread.currentThread(), this, LOCK.EXCLUSIVE);
			try {
				countReentrantWrite.incrementAndGet();
				read();
				// try {
				// Thread.sleep(1 + Math.abs(new Random().nextInt() % 3));
				// } catch (Exception e) {
				// }
				if (verbose)
					System.out.println("ResourceReantrance locked by " + Thread.currentThread());
				if (countReentrantWrite.get() != 1)
					throw new AssertionError("countReentrantWrite:" + countReentrantWrite);
			} finally {
				countReentrantWrite.decrementAndGet();
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

		final long start = System.currentTimeMillis();

		// for (int i = 0; i < 10; i++)
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

		System.out.println("\nOk, all threads back : " + counter.get() + " in: " + ((System.currentTimeMillis() - start) / 1000f)
				+ " secs");

		// Pulish exceptions.
		if (exceptions.size() > 0) {
			for (Throwable exc : exceptions) {
				exc.printStackTrace();
			}
			throw exceptions.get(0);
		}

		Assert.assertEquals(counter.get(), processes.size() * resources.size() * cyclesByProcess);

		Assert.assertEquals(lockMgr.getCountCurrentLocks(), 0);
	}
}
