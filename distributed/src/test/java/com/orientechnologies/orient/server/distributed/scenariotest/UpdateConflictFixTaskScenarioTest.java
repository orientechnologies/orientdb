/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.orient.server.distributed.scenariotest;

import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server (quorum=2)
 * - write on server1, quorum responses:
 *      - server1 ok
 *      - server2 ok
 *      - server3 no
 * - quorum reached
 * - wait for fix task
 * - two writes on server3:check consistency on all the servers (the fix task send to server3 the accepted operation)
 */

public class UpdateConflictFixTaskScenarioTest extends AbstractScenarioTest {

  @Test
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // execute writes only on server3
    executeWritesOnServers.addAll(serverInstance);

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    /*
         * Test with quorum = 2
         */

    banner("Test with quorum = 2");

    // CRUD operation on server1


    // server3 response: NO


    // check consistency



  }

}
