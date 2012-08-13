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

import com.orientechnologies.common.directmemory.OBuddyMemory;
import com.orientechnologies.common.directmemory.collections.ODirectMemoryList;
import com.orientechnologies.common.serialization.types.OStringSerializer;

/**
 * @author Andrey Lomakin
 * @since 12.08.12
 */
public class DirectMemoryListSpeedTest extends CollectionBaseAbstractSpeedTest {
  private ODirectMemoryList<String> arrayList;

  public DirectMemoryListSpeedTest() {
    super(10000000);
  }

  @Override
  public void cycle() {
    for (String item : arrayList) {
      if (item.equals(searchedValue))
        break;
    }
  }

  @Override
  public void init() {
    arrayList = new ODirectMemoryList<String>(new OBuddyMemory(100 * 1024 * 1024, 32), OStringSerializer.INSTANCE);
    for (int i = 0; i < collectionSize; ++i) {
      arrayList.add(String.valueOf(i));
    }
  }

  public void deInit() {
    arrayList.clear();
    arrayList = null;
  }
}
