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

import java.io.IOException;
import java.util.Locale;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
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

	public void testDbCreationNoSecurity() throws IOException {
		if (url.startsWith(OEngineMemory.NAME))
			OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);

		if (!url.startsWith(OEngineRemote.NAME)) {
			database = new ODatabaseObjectTx(url);
			database.setProperty("security", Boolean.FALSE);
			TestUtils.createDatabase(database, url);
			TestUtils.deleteDatabase(database);
		}
	}

	@Test(dependsOnMethods = { "testDbCreationNoSecurity" })
	public void testDbCreationDefault() throws IOException {
		if (url.startsWith(OEngineMemory.NAME))
			OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);

		TestUtils.createDatabase(new ODatabaseObjectTx(url), url);
	}

	@Test(dependsOnMethods = { "testDbCreationDefault" })
	public void testDbExists() throws IOException {
		Assert.assertTrue(TestUtils.existsDatabase(new ODatabaseDocumentTx(url)));
	}

	@Test(dependsOnMethods = { "testDbExists" })
	public void testDbOpen() {
		database = new ODatabaseObjectTx(url);
		database.open("admin", "admin");
		database.close();
	}

	@Test(dependsOnMethods = { "testDbOpen" })
	public void testDbOpenWithLastAsSlash() {
		database = new ODatabaseObjectTx(url + "/");
		database.open("admin", "admin");
		database.close();
	}

	@Test(dependsOnMethods = { "testDbOpenWithLastAsSlash" })
	public void testDbOpenWithBackSlash() {
		database = new ODatabaseObjectTx(url.replace('/', '\\'));
		database.open("admin", "admin");
		database.close();
	}

	@Test(dependsOnMethods = { "testDbOpenWithBackSlash" })
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
