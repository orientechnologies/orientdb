/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.stresstest.OStressTester;
import com.orientechnologies.orient.stresstest.OStressTesterCommandLineParser;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Executed distributed stress tests. In particular executes a load-balanced (per request) insertion
 * in a graph, distributed on 3 nodes, with 8 parallel clients in total, trying to build many
 * vertices all connected to the same super node. This means very high contention on update.
 *
 * <p>This test has been created to reproduce a weird problem when the UNDO operation left the RID
 * nagative in Ridbag.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class DistributedStressIT extends AbstractServerClusterTest {
  private static final int SERVERS = 3;

  public String getDatabaseName() {
    return "DistributedStressIT";
  }

  @Test
  @Ignore
  // this test uses GINSERT workload, that is in the old graphdb (TP2) module, so it cannot run
  // anymore.
  // TODO migrate to the new multi-model API
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    // -m remote -c 8 -tx 50 -w GINSERT:V100000F20Ssupernode --remote-ip localhost --root-password
    // root --ha-metrics true
    final OStressTester stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-m",
              "remote",
              "-c",
              "8",
              "-tx",
              "50",
              "-w",
              "GINSERT:V500F20Ssupernode",
              "--remote-ip",
              "localhost",
              "--root-password",
              "test",
              "--ha-metrics",
              "true"
            });

    OLogManager.instance().flush();
    System.out.flush();

    System.out.println();

    stressTester.execute();
  }
}
