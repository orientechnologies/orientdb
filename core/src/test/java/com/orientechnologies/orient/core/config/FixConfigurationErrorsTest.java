/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.config;

import com.orientechnologies.common.util.OMemory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Sergey Sitnikov
 */
public class FixConfigurationErrorsTest {

  private int originalCacheSize;
  private int originalChunkSize;

  @Before
  public void before() {
    originalCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsInteger();
    originalChunkSize = OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsInteger();
  }

  @After
  public void after() {
    OGlobalConfiguration.DISK_CACHE_SIZE.setValue(originalCacheSize);
    OGlobalConfiguration.MEMORY_CHUNK_SIZE.setValue(originalChunkSize);
  }

  @Test
  public void testCacheConfigurationErrorsFixed() {
    final long directMemory = OMemory.getConfiguredMaxDirectMemory();

    if (directMemory != -1) {
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(directMemory / 1024 / 1024 + 1);
      OMemory.fixCommonConfigurationProblems();
      Assert.assertTrue(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() <= directMemory / 1024 / 1024);
    } else {
      final long jvmMaxMemory = Runtime.getRuntime().maxMemory();
      Assert.assertTrue(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() <= jvmMaxMemory / 1024 / 1024);
    }
  }

  @Test
  public void testBufferPoolConfigurationErrorsFixed() {
    OGlobalConfiguration.DISK_CACHE_SIZE.setValue(256);
    OGlobalConfiguration.MEMORY_CHUNK_SIZE.setValue((256 + 1) * 1024 * 1024);
    OMemory.fixCommonConfigurationProblems();
    Assert.assertTrue(OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsLong()
        <= OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024);
  }

}
