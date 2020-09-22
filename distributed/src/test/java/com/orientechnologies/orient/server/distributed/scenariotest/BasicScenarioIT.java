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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

/**
 * Basic Test that checks the consistency in the cluster with the following scenario: - 3 server - 5
 * threads for each server write 100 records - check consistency - no faults
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class BasicScenarioIT extends AbstractScenarioTest {
  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    useTransactions = true;
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocument database = getDatabase(0);
    try {
      new ODocument("Customer").fields("name", "Jay", "surname", "Miner").save();
      new ODocument("Customer").fields("name", "Luke", "surname", "Skywalker").save();
      new ODocument("Provider").fields("name", "Yoda", "surname", "Nothing").save();
    } finally {
      database.close();
    }

    executeMultipleWrites(super.executeTestsOnServers, "plocal");
    super.checkWritesAboveCluster(serverInstance, executeTestsOnServers);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-basic";
  }
}
