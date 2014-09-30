/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by enricorisa on 19/09/14.
 */
@Test
public abstract class BaseLuceneTest {

  protected ODatabaseDocument   databaseDocumentTx;
  private String                url;
  protected OServer             server;
  private boolean               remote;
  protected ODatabaseDocumentTx serverDatabase;

  private final ExecutorService pool = Executors.newFixedThreadPool(1);

  public BaseLuceneTest() {
    this(false);
  }

  public BaseLuceneTest(boolean remote) {
    this.remote = remote;

  }

  @Test(enabled = false)
  public void initDB() {

    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    if (remote) {
      try {
        final String finalBuildDirectory = buildDirectory;
        Future<Boolean> iServer = pool.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            System.setProperty("ORIENTDB_HOME", finalBuildDirectory);
            server = OServerMain.create();
            server.startup(ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml"));
            server.activate();
            serverDatabase = Orient.instance().getDatabaseFactory()
                .createDatabase("graph", getStoragePath(getDatabaseName(), "plocal"));
            if (serverDatabase.exists()) {
              serverDatabase.open("admin", "admin");
              serverDatabase.drop();
            }
            serverDatabase.create();
            return true;
          }
        });

        iServer.get();
        url = "remote:localhost/" + getDatabaseName();
        databaseDocumentTx = new ODatabaseDocumentTx(url);
        databaseDocumentTx.open("admin", "admin");
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      url = "plocal:" + buildDirectory + "/" + getDatabaseName();
      databaseDocumentTx = new ODatabaseDocumentTx(url);
      if (!databaseDocumentTx.exists()) {
        databaseDocumentTx = Orient.instance().getDatabaseFactory().createDatabase("graph", url);
        databaseDocumentTx.create();
      } else {
        databaseDocumentTx.open("admin", "admin");
      }
    }

  }

  protected void restart() {

    if (server != null) {

      databaseDocumentTx.close();
      try {
        Future<Boolean> iServer = pool.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {

            server.shutdown();
            server = OServerMain.create();

            server.startup(ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml"));
            server.activate();

            return true;
          }
        });
        iServer.get();
        url = "remote:localhost/" + getDatabaseName();

        databaseDocumentTx = new ODatabaseDocumentTx(url);
        databaseDocumentTx.open("admin", "admin");
        ODatabaseRecordThreadLocal.INSTANCE
            .set((com.orientechnologies.orient.core.db.record.ODatabaseRecordInternal) databaseDocumentTx);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  protected String getStoragePath(final String databaseName, final String storageMode) {
    final String path;
    if (storageMode.equals(OEngineLocalPaginated.NAME)) {
      path = storageMode + ":${" + Orient.ORIENTDB_HOME + "}/databases/" + databaseName;
    } else if (storageMode.equals(OEngineMemory.NAME)) {
      path = storageMode + ":" + databaseName;
    } else {
      return null;
    }
    return path;
  }

  @Test(enabled = false)
  public void deInitDB() {
    if (remote) {
      if (serverDatabase.exists()) {
        if (!serverDatabase.isClosed()) {
          serverDatabase.drop();
        } else {
          serverDatabase = Orient.instance().getDatabaseFactory()
              .createDatabase("graph", getStoragePath(getDatabaseName(), "plocal"));
          serverDatabase.open("admin", "admin");
          serverDatabase.drop();
        }

      }
      if (server != null) {
        server.shutdown();
      }

    } else {
      databaseDocumentTx.drop();
    }
  }

  protected abstract String getDatabaseName();
}
