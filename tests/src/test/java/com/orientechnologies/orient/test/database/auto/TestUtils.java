/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.OConfigurationException;

public class TestUtils {
	public static void createDatabase(ODatabase database, final String iURL) throws IOException {
		if (iURL.startsWith(OEngineRemote.NAME)) {
			new OServerAdmin(iURL).connect("root", getServerRootPassword()).createDatabase("local").close();
		} else {
			database.create();
			database.close();
		}
	}

	public static void deleteDatabase(final ODatabase database) throws IOException {
		if (database.getURL().startsWith("remote:")) {
			new OServerAdmin(database.getURL()).connect("root", getServerRootPassword()).deleteDatabase();
		} else {
			database.delete();
		}
	}

	public static boolean existsDatabase(final ODatabase database) throws IOException {
		if (database.getURL().startsWith("remote")) {
			return new OServerAdmin(database.getURL()).connect("root", getServerRootPassword()).existsDatabase();
		} else {
			return database.exists();
		}
	}

	protected static String getServerRootPassword() throws IOException {
		// LOAD SERVER CONFIG FILE TO EXTRACT THE ROOT'S PASSWORD
		File file = new File("../releases/" + OConstants.ORIENT_VERSION + "/config/orientdb-server-config.xml");
		if (!file.exists())
			file = new File("server/config/orientdb-server-config.xml");
		if (!file.exists())
			file = new File("../server/config/orientdb-server-config.xml");
		if (!file.exists())
			file = new File(OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/orientdb-server-config.xml"));
		if (!file.exists())
			throw new OConfigurationException("Can't load file orientdb-server-config.xml to execute remote tests");

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
