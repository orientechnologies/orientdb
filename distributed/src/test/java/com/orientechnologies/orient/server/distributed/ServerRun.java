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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.server.OServer;

/**
 * Running server instance.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ServerRun {
  protected String       rootPath;
  protected final String serverId;
  protected OServer      server;

  public ServerRun(final String iRootPath, final String serverId) {
    this.rootPath = iRootPath;
    this.serverId = serverId;
  }

  protected void createDatabase(final String iName) {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);

    String dbPath = getDatabasePath(iName);

    System.out.println("Creating database " + iName + " under: " + dbPath + "...");

    new File(dbPath).mkdirs();

    final ODatabaseDocumentTx database = new ODatabaseDocumentTx("local:" + dbPath);
    if (database.exists())
      OFileUtils.deleteRecursively(new File(dbPath));

    database.create();

    System.out.println("Creating database schema...");

    // CREATE BASIC SCHEMA
    OClass personClass = database.getMetadata().getSchema().createClass("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("firstName", OType.STRING);
    personClass.createProperty("lastName", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.INTEGER);

    database.close();
  }

  protected void copyDatabase(final String iDatabaseName, final String iDestinationDirectory) throws IOException {
    // COPY THE DATABASE TO OTHER DIRECTORIES
    System.out.println("Copying database " + iDatabaseName + " to " + iDestinationDirectory + "...");

    OFileUtils.deleteRecursively(new File(iDestinationDirectory));
    OFileUtils.copyDirectory(new File(getDatabasePath(iDatabaseName)), new File(iDestinationDirectory));
  }

  public OServer getServerInstance() {
    return server;
  }

  public String getServerId() {
    return serverId;
  }

  protected OServer startServer(final String iConfigFile) throws Exception, InstantiationException, IllegalAccessException,
      ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IOException {
    System.out.println("Starting server " + serverId + " from " + getServerHome() + "...");

    System.setProperty("ORIENTDB_HOME", getServerHome());

    server = new OServer();
    server.startup(getClass().getClassLoader().getResourceAsStream(iConfigFile));
    server.activate();
    return server;
  }

  protected void shutdownServer() {
    if (server != null)
      server.shutdown();
  }

  protected String getServerHome() {
    return getServerHome(serverId);
  }

  protected String getDatabasePath(final String iDatabaseName) {
    return getDatabasePath(serverId, iDatabaseName);
  }

  public static String getServerHome(final String iServerId) {
    return "target/server" + iServerId;
  }

  public static String getDatabasePath(final String iServerId, final String iDatabaseName) {
    return getServerHome(iServerId) + "/databases/" + iDatabaseName;
  }
}
