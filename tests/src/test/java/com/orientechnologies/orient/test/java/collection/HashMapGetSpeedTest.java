/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *  
 */
package com.orientechnologies.orient.test.java.collection;

import java.util.HashMap;
import java.util.UUID;

public class HashMapGetSpeedTest {
  private final static int MAX_ENTRIES = 32;
  private final static int MAX_LOOP    = 10000000;
  private final static int TOT_LAPS    = 10;

  public static void main(String[] args) {
    final HashMap<String, String> map = new HashMap<String, String>(MAX_ENTRIES);

    final String[] values = new String[MAX_ENTRIES];
    for (int i = 0; i < MAX_ENTRIES; ++i)
      values[i] = UUID.randomUUID().toString();

    for (int i = 0; i < MAX_ENTRIES; ++i)
      map.put(values[i], "test" + i);

    long totalTime = 0;
    for (int test = 0; test < TOT_LAPS; test++) {
      final long start = System.currentTimeMillis();

      for (int i = 0; i < MAX_LOOP; ++i)
        for (int k = 0; k < MAX_ENTRIES; ++k) {

          map.get(values[k]);
        }

      final long elapsed = System.currentTimeMillis() - start;
      totalTime += elapsed;

      System.out.println("Lap time: " + elapsed);
    }
    System.out.println("Total time: " + totalTime + ", average per lap: " + (totalTime / TOT_LAPS));
  }
}
