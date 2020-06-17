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

package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import junit.framework.Assert;
import org.junit.Test;

/** Check vertex and edge creation are propagated across all the nodes in asynchronous mode. */
public class ServerClusterAsyncGraphIT extends AbstractServerClusterTest {
  static final int SERVERS = 2;
  private OVertex v1;
  private OVertex v2;
  private OVertex v3;

  public String getDatabaseName() {
    return "distributed-graphtest";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "asynch-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  protected void executeTest() throws Exception {
    {
      OrientDB orientdb = serverInstance.get(0).getServerInstance().getContext();
      orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
      ODatabaseDocument g = orientdb.open(getDatabaseName(), "admin", "admin");

      try {
        g.createClass("Post", "V");
        g.createClass("User", "V");
        g.createClass("Own", "E");

        g.newVertex("User").save();

        g.command(new OCommandSQL("insert into Post (content, timestamp) values('test', 1)"))
            .execute();
      } finally {
        g.close();
      }
    }

    // CHECK VERTEX CREATION ON ALL THE SERVERS
    for (int s = 0; s < SERVERS; ++s) {
      OrientDB orientdb = serverInstance.get(s).getServerInstance().getContext();
      orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
      ODatabaseDocument g2 = orientdb.open(getDatabaseName(), "admin", "admin");

      try {

        Iterable<OElement> result = g2.command(new OCommandSQL("select from Post")).execute();
        Assert.assertTrue(result.iterator().hasNext());
        Assert.assertNotNull(result.iterator().next());

      } finally {
        g2.close();
      }
    }

    {
      OrientDB orientdb = serverInstance.get(0).getServerInstance().getContext();
      orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
      ODatabaseDocument g = orientdb.open(getDatabaseName(), "admin", "admin");
      try {
        g.command(
                new OCommandSQL("create edge Own from (select from User) to (select from Post)")
                    .onAsyncReplicationError(
                        new OAsyncReplicationError() {
                          @Override
                          public ACTION onAsyncReplicationError(Throwable iException, int iRetry) {
                            return iException instanceof ONeedRetryException && iRetry <= 3
                                ? ACTION.RETRY
                                : ACTION.IGNORE;
                          }
                        }))
            .execute();

      } finally {
        g.close();
      }
    }

    Thread.sleep(1000);

    // CHECK VERTEX CREATION ON ALL THE SERVERS
    for (int s = 0; s < SERVERS; ++s) {
      OrientDB orientdb = serverInstance.get(s).getServerInstance().getContext();
      orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
      ODatabaseDocument g2 = orientdb.open(getDatabaseName(), "admin", "admin");

      try {

        Iterable<OVertex> result = g2.command(new OCommandSQL("select from Own")).execute();
        Assert.assertTrue(result.iterator().hasNext());
        Assert.assertNotNull(result.iterator().next());

        result = g2.command(new OCommandSQL("select from Post")).execute();
        Assert.assertTrue(result.iterator().hasNext());

        final OElement v = result.iterator().next();
        Assert.assertNotNull(v);

        final Iterable<OEdge> inEdges = v.asVertex().get().getEdges(ODirection.IN);
        Assert.assertTrue(inEdges.iterator().hasNext());
        Assert.assertNotNull(inEdges.iterator().next());

        result = g2.command(new OCommandSQL("select from User")).execute();
        Assert.assertTrue(result.iterator().hasNext());

        final OElement v2 = result.iterator().next();
        Assert.assertNotNull(v2);

        final Iterable<OEdge> outEdges = v2.asVertex().get().getEdges(ODirection.OUT);
        Assert.assertTrue(outEdges.iterator().hasNext());
        Assert.assertNotNull(outEdges.iterator().next());

      } finally {
        g2.close();
      }
    }
  }
}
