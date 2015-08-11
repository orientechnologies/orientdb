/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import junit.framework.Assert;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * Distributed test on drop database and further create that must raise an error.
 */
public class DistributedDbDropCannotCreateTest extends AbstractServerClusterTxTest {
  final static int SERVERS = 2;

  @Test
  public void test() throws Exception {
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    for (ServerRun s : serverInstance) {
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(s));
      db.open("admin", "admin");

      banner("DROPPING DATABASE ON SERVER " + s.getServerId());
      db.drop();

      banner("RE-CREATING DATABASE ON SERVER " + s.getServerId());
      try {
        db.create();
        Assert.fail("Creation of database after drop was allowed");

      } catch (ODatabaseException e) {
        Assert.assertTrue(e.getCause() instanceof ODistributedException);
        Assert.assertEquals(e.getCause().getMessage(),
            "Cannot create a new database with the same name of one available distributed");
      }
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-dropnocreatedb";
  }
}
