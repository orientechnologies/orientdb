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
package com.orientechnologies.orient.client.db;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.OConfigurationException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ODatabaseHelper {
  public static void createDatabase(ODatabase database, final String iURL) throws IOException {
    createDatabase(database, iURL, "server");
  }

  public static void createDatabase(ODatabase database, final String iURL, String iDirectory) throws IOException {
    if (iURL.startsWith(OEngineRemote.NAME)) {
      new OServerAdmin(iURL).connect("root", getServerRootPassword(iDirectory)).createDatabase("document", "local").close();
    } else {
      database.create();
      database.close();
    }
  }

  public static void deleteDatabase(final ODatabase database) throws IOException {
    deleteDatabase(database, "server");
  }

  @Deprecated
  public static void deleteDatabase(final ODatabase database, final String iDirectory) throws IOException {
    dropDatabase(database, iDirectory);
  }

  public static void dropDatabase(final ODatabase database) throws IOException {
    dropDatabase(database, "server");
  }

  public static void dropDatabase(final ODatabase database, final String iDirectory) throws IOException {
    if (database.getURL().startsWith("remote:")) {
      new OServerAdmin(database.getURL()).connect("root", getServerRootPassword(iDirectory)).dropDatabase();
    } else {
      if (existsDatabase(database)) {
        if (database.isClosed())
          database.open("admin", "admin");
        database.drop();
      }
    }
  }

  public static boolean existsDatabase(final ODatabase database) throws IOException {
    if (database.getURL().startsWith("remote")) {
      return new OServerAdmin(database.getURL()).connect("root", getServerRootPassword()).existsDatabase();
    } else {
      return database.exists();
    }
  }

	public static void freezeDatabase(final ODatabase database) throws IOException {
		if (database.getURL().startsWith("remote")) {
			final OServerAdmin serverAdmin = new OServerAdmin(database.getURL());
			serverAdmin.connect("root", getServerRootPassword()).freezeDatabase();
			serverAdmin.close();
		} else {
			database.freeze();
		}
	}

	public static void releaseDatabase(final ODatabase database) throws IOException {
		if (database.getURL().startsWith("remote")) {
			final OServerAdmin serverAdmin = new OServerAdmin(database.getURL());
			serverAdmin.connect("root", getServerRootPassword()).releaseDatabase();
			serverAdmin.close();
		} else {
			database.release();
		}
	}


	protected static String getServerRootPassword() throws IOException {
    return getServerRootPassword("server");
  }

  protected static String getServerRootPassword(final String iDirectory) throws IOException {
    // LOAD SERVER CONFIG FILE TO EXTRACT THE ROOT'S PASSWORD
    File file = new File("../releases/orientdb-" + OConstants.ORIENT_VERSION + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file = new File("../../releases/orientdb-" + OConstants.ORIENT_VERSION + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file = new File(iDirectory + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file = new File("../" + iDirectory + "/config/orientdb-server-config.xml");
    if (!file.exists())
      file = new File(OSystemVariableResolver.resolveSystemVariables("${" + Orient.ORIENTDB_HOME
          + "}/config/orientdb-server-config.xml"));
    if (!file.exists())
      throw new OConfigurationException(
          "Cannot load file orientdb-server-config.xml to execute remote tests. Current directory is "
              + new File(".").getAbsolutePath());

    FileReader f = new FileReader(file);
    final char[] buffer = new char[(int) file.length()];
    f.read(buffer);
    f.close();

    String fileContent = new String(buffer);
    int pos = fileContent.indexOf("password=\"");
    pos += "password=\"".length();
    return fileContent.substring(pos, fileContent.indexOf('"', pos));
  }
}
