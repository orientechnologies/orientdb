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

package com.orientechnologies.orient.test.java.collection;

import java.util.Random;

import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.OBuddyMemory;
import com.orientechnologies.common.directmemory.collections.ODirectMemoryHashMap;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

/**
 * @author Andrey Lomakin
 * @since 19.08.12
 */
public class DirectMemoryHashMapSpeedTest extends CollectionBaseAbstractSpeedTest {
  private static final int                            COUNT  = 1000000;
  private static final ORID                           rid    = new ORecordId(10, OClusterPositionFactory.INSTANCE.valueOf(100));

  private ODirectMemoryHashMap<String, OIdentifiable> hashMap;
  private Random                                      random = new Random();
  private OBuddyMemory                                memory;

  public DirectMemoryHashMapSpeedTest() {
    super(COUNT);
  }

  @Override
  @Test(enabled = false)
  public void cycle() {
    hashMap.get(String.valueOf(random.nextInt(COUNT)));
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    memory = new OBuddyMemory(1000000000, 32);
    hashMap = new ODirectMemoryHashMap<String, OIdentifiable>(memory, OLinkSerializer.INSTANCE, OStringSerializer.INSTANCE);

    for (int i = 0; i < COUNT; i++)
      hashMap.put(String.valueOf(random.nextInt(COUNT)), rid);

    System.gc();
    Thread.sleep(500);
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    System.out.println("\nFree memory " + memory.freeSpace() + " bytes");
  }
}
