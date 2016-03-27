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
package com.orientechnologies.orient.client.db;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ODatabaseHelper {
  public static void createDatabase(ODatabase database, final String url) throws IOException {
    createDatabase(database, url, "server", "plocal");
  }

  public static void createDatabase(ODatabase database, final String url, String type) throws IOException {
    createDatabase(database, url, "server", type);
  }

  public static void createDatabase(ODatabase database, final String url, String directory, String type) throws IOException {
    if (url.startsWith(OEngineRemote.NAME)) {
      new OServerAdmin(url).connect("root", getServerRootPassword(directory)).createDatabase("document", type).close();
    } else {
      database.create();
      database.close();
    }
  }

  public static void deleteDatabase(final ODatabase database, String storageType) throws IOException {
    deleteDatabase(database, "server", storageType);
  }

  @Deprecated
  public static void deleteDatabase(final ODatabase database, final String directory, String storageType) throws IOException {
    dropDatabase(database, directory, storageType);
  }

  public static void dropDatabase(final ODatabase database, String storageType) throws IOException {
    dropDatabase(database, "server", storageType);
  }

  public static void dropDatabase(final ODatabase database, final String directory, String storageType) throws IOException {
    if (existsDatabase(database, storageType)) {
      if (database.getURL().startsWith("remote:")) {
        database.activateOnCurrentThread();
        database.close();
        OServerAdmin admin = new OServerAdmin(database.getURL()).connect("root", getServerRootPassword(directory));
        admin.dropDatabase(storageType);
        admin.close();
      } else {
        if (database.isClosed())
          database.open("admin", "admin");
        else
          database.activateOnCurrentThread();
        database.drop();
      }
    }
  }

  public static boolean existsDatabase(final ODatabase database, String storageType) throws IOException {
    database.activateOnCurrentThread();
    if (database.getURL().startsWith("remote")) {
      OServerAdmin admin = new OServerAdmin(database.getURL()).connect("root", getServerRootPassword());
      boolean exist = admin.existsDatabase(storageType);
      admin.close();
      return exist;
    }

    return database.exists();
  }

  public static boolean existsDatabase(final String url) throws IOException {
    if (url.startsWith("remote")) {
      OServerAdmin admin = new OServerAdmin(url).connect("root", getServerRootPassword());
      boolean exist = admin.existsDatabase();
      admin.close();
      return exist;
    }
    return new ODatabaseDocumentTx(url).exists();
  }

  public static void freezeDatabase(final ODatabase database) throws IOException {
    database.activateOnCurrentThread();
    if (database.getURL().startsWith("remote")) {
      final OServerAdmin serverAdmin = new OServerAdmin(database.getURL());
      serverAdmin.connect("root", getServerRootPassword()).freezeDatabase("plocal");
      serverAdmin.close();
    } else {
      database.freeze();
    }
  }

  public static void releaseDatabase(final ODatabase database) throws IOException {
    database.activateOnCurrentThread();
    if (database.getURL().startsWith("remote")) {
      final OServerAdmin serverAdmin = new OServerAdmin(database.getURL());
      serverAdmin.connect("root", getServerRootPassword()).releaseDatabase("plocal");
      serverAdmin.close();
    } else {
      database.release();
    }
  }

  public static File getConfigurationFile() {
    return getConfigurationFile(null);
  }

  public static String getServerRootPassword() throws IOException {
    return getServerRootPassword("server");
  }

  protected static String getServerRootPassword(final String iDirectory) throws IOException {
    File file = getConfigurationFile(iDirectory);

    FileReader f = new FileReader(file);
    final char[] buffer = new char[(int) file.length()];
    f.read(buffer);
    f.close();

    String fileContent = new String(buffer);
    // TODO search is wrong because if first user is not root tests will fail
    int pos = fileContent.indexOf("password=\"");
    pos += "password=\"".length();
    return fileContent.substring(pos, fileContent.indexOf('"', pos));
  }

  protected static File getConfigurationFile(final String iDirectory) {
    // LOAD SERVER CONFIG FILE TO EXTRACT THE ROOT'S PASSWORD
    String sysProperty = System.getProperty("orientdb.config.file");
    File file = new File(sysProperty != null ? sysProperty : "");
    if (!file.exists()) {
      sysProperty = System.getenv("CONFIG_FILE");
      file = new File(sysProperty != null ? sysProperty : "");
    }
    if (!file.exists())
      file = new File("../releases/orientdb-" + OConstants.ORIENT_VERSION + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file = new File("../releases/orientdb-community-" + OConstants.ORIENT_VERSION + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file = new File("../../releases/orientdb-" + OConstants.ORIENT_VERSION + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file = new File("../../releases/orientdb-community-" + OConstants.ORIENT_VERSION + "/config/orientdb-server-config.xml");
    if (!file.exists() && iDirectory != null) {
      file = new File(iDirectory + "/config/orientdb-server-config.xml");
      if (!file.exists())
        file = new File("../" + iDirectory + "/config/orientdb-server-config.xml");
    }
    if (!file.exists())
      file = new File(OSystemVariableResolver.resolveSystemVariables("${" + Orient.ORIENTDB_HOME
          + "}/config/orientdb-server-config.xml"));
    if (!file.exists())
      throw new OConfigurationException(
          "Cannot load file orientdb-server-config.xml to execute remote tests. Current directory is "
              + new File(".").getAbsolutePath());
    return file;
  }
}
