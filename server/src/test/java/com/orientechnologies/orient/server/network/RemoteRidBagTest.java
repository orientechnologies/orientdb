/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ORecordContentNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Sergey Sitnikov
 */
public class RemoteRidBagTest {

  private static final String SERVER_DIRECTORY = "./target/db";
  private OServer server;

  private ExecutorService clientA;
  private ExecutorService clientB;

  private ODatabaseDocumentTx dbA;
  private ODatabaseDocumentTx dbB;

  private ODocument docA;
  private ODocument docB;

  @Before
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    final OServerAdmin serverAdmin = new OServerAdmin("remote:localhost");
    serverAdmin.connect("root", "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    serverAdmin.createDatabase("RemoteRidBagTest", "document", "memory");

    clientA = Executors.newSingleThreadExecutor();
    clientB = Executors.newSingleThreadExecutor();

    clientA.submit(new Runnable() {
      @Override
      public void run() {
        dbA = new ODatabaseDocumentTx("remote:localhost/RemoteRidBagTest").open("admin", "admin");
      }
    }).get();
    clientB.submit(new Runnable() {
      @Override
      public void run() {
        dbB = new ODatabaseDocumentTx("remote:localhost/RemoteRidBagTest").open("admin", "admin");
      }
    }).get();
  }

  @After
  public void after() throws Exception {
    clientA.submit(new Runnable() {
      @Override
      public void run() {
        dbA.close();
      }
    }).get();
    clientB.submit(new Runnable() {
      @Override
      public void run() {
        dbB.close();
      }
    }).get();

    clientA.shutdown();
    clientB.shutdown();

    server.shutdown();
    Orient.instance().startup();
  }

  @Test
  public void testTwoClients() throws Exception {
    clientA.submit(new Runnable() {
      @Override
      public void run() {
        final ORidBag ridBag = new ORidBag(-1, -1);
        ridBag.setAutoConvertToRecord(false);
        ridBag.add(new ORecordId(2, 2));
        docA = dbA.newInstance().field("bag", ridBag).save();
      }
    }).get();

    clientB.submit(new Runnable() {
      @Override
      public void run() {
        docB = dbB.load(docA.getIdentity());
      }
    }).get();

    clientA.submit(new Runnable() {
      @Override
      public void run() {
        docA.delete();
      }
    }).get();

    clientB.submit(new Runnable() {
      @Override
      public void run() {
        final ORidBag ridBag = docB.field("bag");
        ridBag.setAutoConvertToRecord(false);

        try {
          ridBag.size();
          Assert.fail("Should fail with ORecordContentNotFoundException");
        } catch (ORecordContentNotFoundException e) {
          // eaten
        }
      }
    }).get();
  }

}
