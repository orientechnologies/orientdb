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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Ignore;
import org.junit.Test;

/** Distributed TX test against "plocal" protocol. */
public class LocalConcurrentTxAutoRetryIT extends AbstractDistributedConcurrentTxTest {
  private static final int SERVERS = 3;

  @Test
  @Ignore
  public void test() throws Exception {
    expectedConcurrentException = false;

    final int oldAutoRetry =
        OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.getValueAsInteger();
    OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.setValue(100);

    // final int oldLockTimeout =
    // OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsInteger();
    //  OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(0);

    try {

      init(SERVERS);
      prepare(false);
      execute();

    } finally {
      // OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.setValue(oldLockTimeout);
      OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.setValue(oldAutoRetry);
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-conc-txautoretry";
  }
}
