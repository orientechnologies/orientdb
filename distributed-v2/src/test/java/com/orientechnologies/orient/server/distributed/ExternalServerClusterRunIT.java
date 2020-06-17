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

package com.orientechnologies.orient.server.distributed;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Start 3 servers and wait for external commands. This test must be ignored and activated only upon
 * request for local debugging.
 */
public class ExternalServerClusterRunIT extends AbstractServerClusterTest {
  public String getDatabaseName() {
    return getClass().getSimpleName();
  }

  @Test
  @Ignore
  public void test() throws Exception {
    init(3);
    prepare(false, false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    Thread.sleep(Long.MAX_VALUE);
  }
}
