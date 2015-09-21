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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.OOrientListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by enricorisa on 19/09/14.
 */
@Test
public abstract class BaseLuceneTest {

  protected ODatabaseDocumentTx databaseDocumentTx;
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
    initDB(true);
  }

  @Test(enabled = false)
  public void initDB(boolean drop) {

    buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    if (remote) {
      try {

        startServer(drop);

        url = "remote:localhost/" + getDatabaseName();
        databaseDocumentTx = new ODatabaseDocumentTx(url);
        databaseDocumentTx.open("admin", "admin");
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      url = "plocal:" + buildDirectory + "/databases/" + getDatabaseName();
      databaseDocumentTx = new ODatabaseDocumentTx(url);

      if (databaseDocumentTx.exists()) {
        databaseDocumentTx.open("admin", "admin");
        if (drop) {
          // DROP AND RE-CREATE IT
          databaseDocumentTx.drop();
          databaseDocumentTx = Orient.instance().getDatabaseFactory().createDatabase("graph", url);
          databaseDocumentTx.create();
        }
      } else {
        // CREATE IT
        databaseDocumentTx = Orient.instance().getDatabaseFactory().createDatabase("graph", url);
        databaseDocumentTx.create();
      }
      ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTx);
    }
  }

  protected void startServer(boolean drop) throws IOException, InterruptedException {
    String javaExec = System.getProperty("java.home") + "/bin/java";
    System.setProperty("ORIENTDB_HOME", buildDirectory);

    // "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-classpath", System.getProperty("java.class.path"),
        "-DORIENTDB_HOME=" + buildDirectory, RemoteDBRunner.class.getName(), getDatabaseName(), "" + drop);

    process = processBuilder.start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line;
    do {
      line = reader.readLine();
    } while (!"started".equals(line));
  }

  protected void kill(boolean soft) {

    if (!soft) {
      try {
        int pid = getUnixPID(process);
        Runtime.getRuntime().exec("kill -9 " + pid).waitFor();
      } catch (Exception e) {
        process.destroy();
      }
    } else {
      process.destroy();
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("Process was destroyed");

  }

  public static int getUnixPID(Process process) throws Exception {
    System.out.println(process.getClass().getName());
    if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
      Class cl = process.getClass();
      Field field = cl.getDeclaredField("pid");
      field.setAccessible(true);
      Object pidObject = field.get(process);
      return (Integer) pidObject;
    } else {
      throw new IllegalArgumentException("Needs to be a UNIXProcess");
    }
  }

  public static int killUnixProcess(Process process) throws Exception {
    int pid = getUnixPID(process);
    return Runtime.getRuntime().exec("kill " + pid).waitFor();
  }

  protected void restart() {

    process.destroy();
    try {
      startServer(false);
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
      ODatabaseRecordThreadLocal.INSTANCE.set(null);
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

        if (args.length > 1 && args[1].equals("true")) {
          if (db.exists()) {
            db.open("admin", "admin");
            db.drop();
          }
          db.create();
        }

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
        // Don't remove this is needed for the parent process to understand when the server started
        System.out.println("started");
      }

    }
  }
}
