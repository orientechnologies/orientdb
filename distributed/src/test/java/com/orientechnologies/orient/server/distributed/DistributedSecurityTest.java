/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.server.distributed;

import org.junit.Assert;

import org.junit.Test;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * Tests the behavior of security in distributed configuration.
 */
public class DistributedSecurityTest extends AbstractServerClusterTest {
  private final static int SERVERS = 1;

  public String getDatabaseName() {
    return "distributed-security";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    for (int s = 0; s < SERVERS; ++s) {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + s + "/databases/" + getDatabaseName(), "reader",
          "reader");
      OrientGraphNoTx g = factory.getNoTx();

      try {

        try {
          // TRY DELETING ALL OUSER VIA COMMAND
          g.command(new OCommandSQL("delete from OUser")).execute();
          Assert.assertTrue(false);
        } catch (Exception e) {
          Assert.assertTrue(true);
        }

        try {
          // TRY DELETING CURRENT OUSER VIA API
          g.getRawGraph().getUser().getIdentity().getRecord().delete();
          Assert.assertTrue(false);
        } catch (Exception e) {
          Assert.assertTrue(true);
        }

      } finally {
        g.shutdown();
      }
    }
  }
}
