/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.server.distributed.hz;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

/** Created by luca on 24/09/14. */
public class HazelcastQueueThroughputTest {
  private static final int TOTAL = 1000000;
  private static final int LAP = 100000;
  private static final int QUEUE_RING_SIZE = 8;

  public static void main(String[] args) throws InterruptedException {
    final HazelcastInstance hz = Hazelcast.newHazelcastInstance();

    final IQueue[] ring = new IQueue[QUEUE_RING_SIZE];
    for (int q = 0; q < QUEUE_RING_SIZE; ++q) ring[q] = hz.getQueue("test" + q);

    final long start = System.currentTimeMillis();
    long lastLap = start;

    Thread t =
        new Thread() {
          long lastLap = start;

          @Override
          public void run() {
            int senderQueueIndex = 0;

            System.out.println((System.currentTimeMillis() - lastLap) + " Start receiving msgs");
            for (int i = 1; i < TOTAL + 1; ++i) {
              try {
                if (senderQueueIndex >= QUEUE_RING_SIZE) senderQueueIndex = 0;
                Object msg = ring[senderQueueIndex++].take();

                if (i % LAP == 0) {
                  final long lapTime = System.currentTimeMillis() - lastLap;
                  System.out.printf(
                      "<- messages %d/%d = %dms (%f msg/sec)\n",
                      i, TOTAL, lapTime, ((float) LAP * 1000 / lapTime));
                  lastLap = System.currentTimeMillis();
                }

              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
        };
    t.start();

    int receiverQueueIndex = 0;

    System.out.println((System.currentTimeMillis() - lastLap) + " Start sending msgs");
    for (int i = 1; i < TOTAL + 1; ++i) {
      if (receiverQueueIndex >= QUEUE_RING_SIZE) receiverQueueIndex = 0;
      ring[receiverQueueIndex++].offer(i);

      if (i % LAP == 0) {
        final long lapTime = System.currentTimeMillis() - lastLap;
        System.out.printf(
            "-> messages %d/%d = %dms (%f msg/sec)\n",
            i, TOTAL, lapTime, ((float) LAP * 1000 / lapTime));
        lastLap = System.currentTimeMillis();
      }
    }

    System.out.println((System.currentTimeMillis() - start) + " Finished sending msgs");

    t.join();

    System.out.println((System.currentTimeMillis() - start) + " Test finished");
  }
}
