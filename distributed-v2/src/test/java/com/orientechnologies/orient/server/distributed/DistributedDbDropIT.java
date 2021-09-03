/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.junit.Assert;
import org.junit.Test;

/** Distributed test on drop database. */
public class DistributedDbDropIT extends AbstractServerClusterTxTest {
  static final int SERVERS = 3;
  int serverStarted = 0;

  @Test
  public void test() throws Exception {
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    ServerRun s = serverInstance.get(0);

    banner("DROPPING DATABASE ON SERVER " + s.getServerId());
    s.getServerInstance().dropDatabase(getDatabaseName());

    for (int i = 1; i < serverInstance.size(); ++i) {
      try {
        serverInstance.get(i).getServerInstance().openDatabase(getDatabaseName());
        Assert.fail("The database was not deleted on server " + i);
      } catch (ODatabaseException e) {
        Assert.assertTrue(e.getCause().getMessage().contains("it does not exist"));
      }
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-dropdb";
  }
}
