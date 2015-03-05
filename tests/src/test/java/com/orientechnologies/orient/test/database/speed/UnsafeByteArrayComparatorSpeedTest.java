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

package com.orientechnologies.orient.test.database.speed;

import java.util.Random;

import com.orientechnologies.common.comparator.OUnsafeByteArrayComparator;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 11.07.12
 */
@Test
public class UnsafeByteArrayComparatorSpeedTest extends OrientMonoThreadTest {
  private final OUnsafeByteArrayComparator unsafeByteArrayComparator = new OUnsafeByteArrayComparator();
  private final Random                     random                    = new Random();
  private final byte[]                     bytesOne                  = new byte[256];
  private final byte[]                     bytesTwo                  = new byte[256];

  public UnsafeByteArrayComparatorSpeedTest() {
    super(10000000);
  }

  @Override
  @Test(enabled = false)
  public void beforeCycle() throws Exception {
    int eqBorder = random.nextInt(bytesOne.length);

    for (int i = 0; i < eqBorder; i++) {
      bytesOne[i] = (byte) i;
      bytesTwo[i] = (byte) i;
    }

    bytesOne[eqBorder] = 2;
    bytesTwo[eqBorder] = 3;

  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    unsafeByteArrayComparator.compare(bytesOne, bytesTwo);
  }
}
