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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import junit.framework.Assert;
import org.junit.Test;

/** Tests the behavior of security in distributed configuration. */
public class DistributedSecurityIT extends AbstractServerClusterTest {
  private static final int SERVERS = 1;

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

      ODatabaseDocument g =
          serverInstance
              .get(s)
              .getServerInstance()
              .getContext()
              .open(getDatabaseName(), "admin", "admin");

      try {

        try {
          // TRY DELETING ALL OUSER VIA COMMAND
          Long deleted = g.command(new OCommandSQL("delete from OUser")).execute();
          Assert.assertEquals(deleted.longValue(), 0l);
        } catch (Exception e) {
          Assert.assertTrue(true);
        }

        try {
          // TRY DELETING CURRENT OUSER VIA API
          g.getUser().getIdentity().getRecord().delete();
          Assert.assertTrue(false);
        } catch (Exception e) {
          Assert.assertTrue(true);
        }

      } finally {
        g.close();
      }
    }
  }
}
