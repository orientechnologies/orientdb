/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.concur.lock.OModificationLock;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 15.06.12
 */
@Test
public class StorageModificationLockTest {
  private final static int               THREAD_COUNT     = 100;
	private final static int 							 CYCLES_COUNT     = 20;
  private final AtomicLong               counter          = new AtomicLong();
  private final OModificationLock modificationLock = new OModificationLock();
  private final ExecutorService          executorService  = Executors.newFixedThreadPool(THREAD_COUNT + 1);
  private final List<Future<Void>>             futures          = new ArrayList<Future<Void>>(THREAD_COUNT);
  private final CountDownLatch           countDownLatch   = new CountDownLatch(1);

  @Test
  public void testLock() throws Exception {
    for (int i = 0; i < THREAD_COUNT; i++) {
      futures.add(executorService.submit(new Counter()));
    }

		Future<Void> prohibiter = executorService.submit(new Prohibiter());
    countDownLatch.countDown();
    prohibiter.get();
		for(Future<Void> future : futures)
			future.get();
  }

  private final class Counter implements Callable<Void> {
    private final Random random = new Random();

    public Void call() throws Exception {
      countDownLatch.await();
      for (int n = 0; n < CYCLES_COUNT; n++) {
        final int modificationsCount = random.nextInt(255);
        modificationLock.requestModificationLock();
        try {
          for (int i = 0; i < modificationsCount; i++) {
            counter.incrementAndGet();
            Thread.sleep(random.nextInt(5));
          }
        } finally {
          modificationLock.releaseModificationLock();
        }
      }
      return null;
    }
  }

  private final class Prohibiter implements Callable<Void> {
    public Void call() throws Exception {
      countDownLatch.await();

      for (int n = 0; n < CYCLES_COUNT; n++) {
        modificationLock.prohibitModifications();
        long beforeModification = counter.get();
        Thread.sleep(50);
        if (n % 10 == 0)
          System.out
              .println("After prohibit  modifications " + beforeModification + " before allow modifications " + counter.get());
        Assert.assertEquals(beforeModification, counter.get());
        modificationLock.allowModifications();
        Thread.sleep(50);
      }
			return null;
		}
  }
}
