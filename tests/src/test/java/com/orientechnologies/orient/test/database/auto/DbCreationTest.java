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
import java.util.Locale;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "db")
public class DbCreationTest {
	private String						url;
	private ODatabaseObjectTx	database;

	@Parameters(value = "url")
	public DbCreationTest(String iURL) {
		url = iURL;
		OProfiler.getInstance().startRecording();
	}

	public void testDbCreation() throws IOException {
		if (url.startsWith(OEngineRemote.NAME)) {

			// LAOD SERVER CONFIG FILE TO EXTRACT THE ROOT'S PASSWORD
			File file = new File("../server/config/orientdb-server-config.xml");
			if (!file.exists())
				file = new File("server/config/orientdb-server-config.xml");
			if (!file.exists())
				file = new File(OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/orientdb-server-config.xml"));

			FileReader f = new FileReader(file);
			char[] buffer = new char[(int) file.length()];
			f.read(buffer);
			f.close();

			String fileContent = new String(buffer);
			int pos = fileContent.indexOf("password=\"");
			pos += "password=\"".length();
			String password = fileContent.substring(pos, fileContent.indexOf("\"", pos));

			new OServerAdmin(url).connect("root", password).createDatabase("local").close();
		} else {
			database = new ODatabaseObjectTx(url);
			database.create();
			database.close();
		}
	}

	@Test(dependsOnMethods = { "testDbCreation" })
	public void testDbOpen() {
		database = new ODatabaseObjectTx(url);
		database.open("admin", "admin");
		database.close();
	}

	@Test(dependsOnMethods = { "testDbOpen" })
	public void testChangeLocale() throws IOException {
		database = new ODatabaseObjectTx(url);
		database.open("admin", "admin");
		database.getStorage().getConfiguration().localeLanguage = Locale.ENGLISH.getLanguage();
		database.getStorage().getConfiguration().localeCountry = Locale.ENGLISH.getCountry();
		database.getStorage().getConfiguration().update();
		database.close();
	}

	@Test(dependsOnMethods = { "testChangeLocale" })
	public void testRoles() throws IOException {
		database = new ODatabaseObjectTx(url);
		database.open("admin", "admin");
		database.query(new OSQLSynchQuery<ORole>("select from ORole where name = 'admin'"));
		database.close();
	}
}
