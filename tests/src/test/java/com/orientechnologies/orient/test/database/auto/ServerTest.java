/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.ORemoteConnectionManager;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;

import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

@Test
public class ServerTest extends DocumentDBBaseTest {
  private String serverURL;

  @Parameters(value = "url")
  public ServerTest(@Optional String url) {
    super(url);

    serverURL = url.substring(0, url.lastIndexOf('/'));
  }

  @Test
  public void testDbExists() throws IOException {
    Assert.assertTrue(ODatabaseHelper.existsDatabase(url));
  }

  @Test
  public void testDbList() throws IOException {
    OServerAdmin server = new OServerAdmin(serverURL);
    try {
      server.connect("root", ODatabaseHelper.getServerRootPassword());
      Map<String, String> dbs = server.listDatabases();
      Assert.assertFalse(dbs.isEmpty());
    } finally {
      server.close();
    }
  }

  @Test
  public void testConnectClose() throws IOException {
    ORemoteConnectionManager connManager = (((OEngineRemote) Orient.instance().getRunningEngine("remote"))).getConnectionManager();

    int count = connManager.getAvailableConnections(serverURL);
    OServerAdmin server = new OServerAdmin(serverURL);
    server.connect("root", ODatabaseHelper.getServerRootPassword());
    server.close();

    Assert.assertEquals(connManager.getAvailableConnections(serverURL), count);
  }

  @Test
  public void testOpenCloseCreateClass() throws IOException {

    OServerAdmin admin = new OServerAdmin("remote:localhost/doubleOpenTest");
    admin.connect("root", ODatabaseHelper.getServerRootPassword());
    admin.createDatabase("document", "memory");
    admin.close();

    ODatabaseDocument db = new ODatabaseDocumentTx("remote:localhost/doubleOpenTest");
    try {
      db.open("admin", "admin");
      ODocument d = new ODocument("User");
      d.save();
    } finally {
      db.close();
    }

    try {
      db.open("admin", "admin");
      ODocument d = new ODocument("User");
      d.save();
    } finally {
      db.close();
    }
    admin = new OServerAdmin("remote:localhost/doubleOpenTest");
    admin.connect("root", ODatabaseHelper.getServerRootPassword());
    admin.dropDatabase("memory");
    admin.close();
  }

}
