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

import com.orientechnologies.orient.core.OOrientListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import java.io.IOException;
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
  private Process               process;
  protected String              buildDirectory;
  private final ExecutorService pool = Executors.newFixedThreadPool(1);

  public BaseLuceneTest() {
    this(false);
  }

  public BaseLuceneTest(boolean remote) {
    this.remote = remote;

  }

  @Test(enabled = false)
  public void initDB() {

    buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    if (remote) {
      try {

        startServer();

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

  protected void startServer() throws IOException, InterruptedException {
    String javaExec = System.getProperty("java.home") + "/bin/java";
    System.setProperty("ORIENTDB_HOME", buildDirectory);

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-classpath", System.getProperty("java.class.path"),
        "-DORIENTDB_HOME=" + buildDirectory, RemoteDBRunner.class.getName(), getDatabaseName());
    processBuilder.inheritIO();

    process = processBuilder.start();
    Thread.sleep(5000);
  }

  protected void restart() {

    process.destroy();
    try {
      startServer();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  protected static String getStoragePath(final String databaseName, final String storageMode) {
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
      process.destroy();

    } else {
      databaseDocumentTx.drop();
    }
  }

  protected abstract String getDatabaseName();

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {

      if (args.length > 0) {
        OServer server = OServerMain.create();
        server.startup(ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml"));
        server.activate();
        final ODatabaseDocumentTx db = Orient.instance().getDatabaseFactory()
            .createDatabase("graph", getStoragePath(args[0], "plocal"));

        if (db.exists()) {
          db.open("admin", "admin");
          db.drop();
        }
        db.create();

        Orient.instance().registerListener(new OOrientListener() {
          @Override
          public void onStorageRegistered(OStorage iStorage) {

          }

          @Override
          public void onStorageUnregistered(OStorage iStorage) {

          }

          @Override
          public void onShutdown() {
            db.drop();
          }
        });
        while (true)
          Thread.sleep(1000);
      }

    }
  }
}
