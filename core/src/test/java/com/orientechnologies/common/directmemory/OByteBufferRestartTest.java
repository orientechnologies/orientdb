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

package com.orientechnologies.common.directmemory;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Sergey Sitnikov
 */
@RunWith(Parameterized.class)
public class OByteBufferRestartTest {

  private static final int PAGE_SIZE = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  @Parameterized.Parameters
  public static OByteBufferPool[] pools() {
    return new OByteBufferPool[] { OByteBufferPool.instance(), new OByteBufferPool(PAGE_SIZE, 1024 * 1024, 1024 * 1024) };
  }

  private final OByteBufferPool pool;

  @Before
  public void before() {
    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  @After
  public void after() {
    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  public OByteBufferRestartTest(OByteBufferPool pool) {
    this.pool = pool;
  }

  @Test
  public void testInstance() {
    ByteBuffer byteBuffer = pool.acquireDirect(true);
    pool.release(byteBuffer);

    assertEquals(1, pool.getBuffersInThePool());
    assertTrue(pool.getAllocatedMemory() >= PAGE_SIZE);

    Orient.instance().shutdown();
    assertEquals(0, pool.getBuffersInThePool());
    assertEquals(0, pool.getAllocatedMemory());
    Orient.instance().startup();

    byteBuffer = pool.acquireDirect(true);
    pool.release(byteBuffer);

    assertEquals(1, pool.getBuffersInThePool());
    assertTrue(pool.getAllocatedMemory() >= PAGE_SIZE);
  }

}
