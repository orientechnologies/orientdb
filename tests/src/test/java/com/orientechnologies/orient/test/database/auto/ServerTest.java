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
import com.orientechnologies.orient.client.remote.OServerAdmin;
import junit.framework.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

@Test
public class ServerTest {
  private String url;
  private String serverURL;

  @Parameters(value = "url")
  public ServerTest(String iURL) {
    url = iURL;
    serverURL = url.substring(0, url.lastIndexOf('/'));
  }

  @Test(enabled = false)
  public static void main(String[] args) throws IOException {
    ServerTest test = new ServerTest("remote:localhost/GratefulDeadConcerts");
    test.testDbExists();
    test.testDbList();
  }

  @Test
  public void testDbExists() throws IOException {
    Assert.assertTrue(ODatabaseHelper.existsDatabase(url));
  }

  @Test
  public void testDbList() throws IOException {
    OServerAdmin server = new OServerAdmin(serverURL);
    server.connect("root", ODatabaseHelper.getServerRootPassword());
    Map<String, String> dbs = server.listDatabases();
    Assert.assertFalse(dbs.isEmpty());
  }
}
