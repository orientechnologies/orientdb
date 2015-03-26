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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Start 3 servers and wait for external commands
 */
public class ServerClusterRemoteDocumentTest extends AbstractServerClusterTest {
  final static int SERVERS = 3;

  public String getDatabaseName() {
    return "distributed-remote-docs";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    String id = String.valueOf(Math.random());
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/" + getDatabaseName()).open("admin", "admin");
    db.begin();
    try {
      ODocument client = new ODocument("Client");
      ODocument matter = new ODocument("Matter");
      matter.field("client", client);
      matter.field("id", id);
      List clientMatters = new ArrayList();
      clientMatters.add(matter);
      client.field("matters", clientMatters);
      client.save();
      matter.save();
      db.commit();
    } finally {
      db.close();
    }

    ODatabaseDocumentTx db2 = new ODatabaseDocumentTx("remote:localhost/" + getDatabaseName()).open("admin", "admin");
    db2.begin();
    try {
      ODocument matter = null;
      for (ODocument doc : db2.browseClass("Matter")) {
        if (doc.field("id").equals(id)) {
          matter = doc;
          break;
        }
      }
      if (matter == null) {
        throw new Exception("Matter not found with id" + id);
      }
      matter.field("client", new ODocument());
      matter.save();
      db2.commit();
    } finally {
      db2.close();
    }
  }
}
