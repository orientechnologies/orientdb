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

package com.orientechnologies.common.directmemory.collections;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.orientechnologies.common.directmemory.OBuddyMemory;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 19.08.12
 */
@Test
public class DirectMemoryHashMapPutTest {
  private ODirectMemory                          memory;
  private ODirectMemoryHashMap<Integer, Integer> hashMap;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(16000000, 32);
    hashMap = new ODirectMemoryHashMap<Integer, Integer>(memory, OIntegerSerializer.INSTANCE, OIntegerSerializer.INSTANCE, 2);
  }

  public void testAddOneItem() {
    final int clusterId = 1;

    hashMap.put(clusterId, 10);
    Assert.assertEquals(10, (int) hashMap.get(clusterId));
    Assert.assertEquals(1, hashMap.size());
  }

  public void testAddTwoItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 4;

    hashMap.put(clusterIdOne, 10);
    hashMap.put(clusterIdTwo, 20);

    Assert.assertEquals(10, (int) hashMap.get(clusterIdOne));
    Assert.assertEquals(20, (int) hashMap.get(clusterIdTwo));
    Assert.assertEquals(2, hashMap.size());
  }

  public void testAddThreeItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    hashMap.put(clusterIdOne, 10);
    hashMap.put(clusterIdTwo, 20);
    hashMap.put(clusterIdThree, 30);

    Assert.assertEquals(10, (int) hashMap.get(clusterIdOne));
    Assert.assertEquals(20, (int) hashMap.get(clusterIdTwo));
    Assert.assertEquals(30, (int) hashMap.get(clusterIdThree));
    Assert.assertEquals(3, (int) hashMap.size());
  }

  public void testAddFourItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    hashMap.put(clusterIdOne, 10);
    hashMap.put(clusterIdTwo, 20);
    hashMap.put(clusterIdThree, 30);
    hashMap.put(clusterIdFour, 40);

    Assert.assertEquals(10, (int) hashMap.get(clusterIdOne));
    Assert.assertEquals(20, (int) hashMap.get(clusterIdTwo));
    Assert.assertEquals(30, (int) hashMap.get(clusterIdThree));
    Assert.assertEquals(40, (int) hashMap.get(clusterIdFour));
    Assert.assertEquals(4, hashMap.size());
  }

  public void testAddThreeItemsUpdateOne() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    hashMap.put(clusterIdOne, 10);
    hashMap.put(clusterIdTwo, 20);
    hashMap.put(clusterIdThree, 30);

    hashMap.put(clusterIdOne, 40);

    Assert.assertEquals(40, (int) hashMap.get(clusterIdOne));
    Assert.assertEquals(20, (int) hashMap.get(clusterIdTwo));
    Assert.assertEquals(30, (int) hashMap.get(clusterIdThree));
    Assert.assertEquals(3, hashMap.size());
  }

  public void testAddFourItemsUpdateTwo() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    hashMap.put(clusterIdOne, 10);
    hashMap.put(clusterIdTwo, 20);
    hashMap.put(clusterIdThree, 30);
    hashMap.put(clusterIdFour, 40);

    hashMap.put(clusterIdTwo, 50);
    hashMap.put(clusterIdOne, 60);

    Assert.assertEquals(60, (int) hashMap.get(clusterIdOne));
    Assert.assertEquals(50, (int) hashMap.get(clusterIdTwo));
    Assert.assertEquals(30, (int) hashMap.get(clusterIdThree));
    Assert.assertEquals(40, (int) hashMap.get(clusterIdFour));
    Assert.assertEquals(4, hashMap.size());
  }

  public void testAdd10000RandomItems() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      hashMap.put(clusterId, pointer);
      addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), (int) hashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, hashMap.size());
  }

  public void testAdd10000RandomItemsUpdateHalf() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      hashMap.put(clusterId, pointer);

      if (random.nextInt(2) > 0) {
        hashMap.put(clusterId, pointer / 2);
        addedItems.put(clusterId, pointer / 2);
      } else
        addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), (int) hashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, hashMap.size());
  }
}
