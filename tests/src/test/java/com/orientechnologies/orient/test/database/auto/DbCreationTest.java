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
package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;
import java.util.Locale;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;

@Test(groups = "db")
public class DbCreationTest {
  private String            url;
  private OObjectDatabaseTx database;

  @Parameters(value = "url")
  public DbCreationTest(String iURL) {
    url = iURL;
    Orient.instance().getProfiler().startRecording();
  }

  public void testDbCreationNoSecurity() throws IOException {
    if (url.startsWith(OEngineMemory.NAME))
      OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);

    if (!url.startsWith(OEngineRemote.NAME)) {
      ODatabaseDocument db = new ODatabaseDocumentTx(url);
      db.setProperty("security", Boolean.FALSE);

      ODatabaseHelper.dropDatabase(db, "server");
      ODatabaseHelper.createDatabase(db, url);
      ODatabaseHelper.dropDatabase(db, "server");

      database = new OObjectDatabaseTx(url);
      database.setProperty("security", Boolean.FALSE);

      ODatabaseHelper.dropDatabase(database, "server");
      ODatabaseHelper.createDatabase(database, url);
      ODatabaseHelper.dropDatabase(database, "server");
    }
  }

  @AfterMethod
  public void tearDown() {
    if (url.contains("remote:"))
      ODatabaseDocumentPool.global().close();
  }

  @Test(dependsOnMethods = { "testDbCreationNoSecurity" })
  public void testDbCreationDefault() throws IOException {
    if (url.startsWith(OEngineMemory.NAME))
      OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);

    ODatabaseHelper.createDatabase(new OObjectDatabaseTx(url), url);
  }

  @Test(dependsOnMethods = { "testDbCreationDefault" })
  public void testDbExists() throws IOException {
    Assert.assertTrue(ODatabaseHelper.existsDatabase(new ODatabaseDocumentTx(url)));
  }

  @Test(dependsOnMethods = { "testDbExists" })
  public void testDbOpen() {
    database = new OObjectDatabaseTx(url);
    database.open("admin", "admin");
    Assert.assertNotNull(database.getName());
    database.close();
  }

  @Test(dependsOnMethods = { "testDbOpen" })
  public void testDbOpenWithLastAsSlash() {
    database = new OObjectDatabaseTx(url + "/");
    database.open("admin", "admin");
    database.close();
  }

  @Test(dependsOnMethods = { "testDbOpenWithLastAsSlash" })
  public void testDbOpenWithBackSlash() {
    database = new OObjectDatabaseTx(url.replace('/', '\\'));
    database.open("admin", "admin");
    database.close();
  }

  @Test(dependsOnMethods = { "testDbOpenWithBackSlash" })
  public void testChangeLocale() throws IOException {
    database = new OObjectDatabaseTx(url);
    database.open("admin", "admin");
    database.getStorage().getConfiguration().setLocaleLanguage(Locale.ENGLISH.getLanguage());
    database.getStorage().getConfiguration().setLocaleCountry(Locale.ENGLISH.getCountry());
    database.getStorage().getConfiguration().update();
    database.close();
  }

  @Test(dependsOnMethods = { "testChangeLocale" })
  public void testRoles() throws IOException {
    database = new OObjectDatabaseTx(url);
    database.open("admin", "admin");
    database.query(new OSQLSynchQuery<ORole>("select from ORole where name = 'admin'"));
    database.close();
  }

  @Test(dependsOnMethods = { "testChangeLocale" })
  public void testSubFolderDbCreate() throws IOException {
    int pos = url.lastIndexOf("/");
    String u = url;

    if (pos > -1)
      u = url.substring(0, pos) + "/sub/subTest";
    else {
      pos = url.lastIndexOf(":");
      u = url.substring(0, pos + 1) + "sub/subTest";
    }

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(u);

    try {
      ODatabaseHelper.dropDatabase(db);
    } catch (OStorageException e) {
      Assert.assertTrue(e.getCause().getMessage().equals("Database with name 'sub/subTest' doesn't exits."));
    }
    ODatabaseHelper.createDatabase(db, u);
    db.open("admin", "admin");
    db.close();

    ODatabaseHelper.dropDatabase(db);
  }

  @Test(dependsOnMethods = { "testChangeLocale" })
  public void testSubFolderDbCreateConnPool() throws IOException {
    int pos = url.lastIndexOf("/");
    String u = url;

    if (pos > -1)
      u = url.substring(0, pos) + "/sub/subTest";
    else {
      pos = url.lastIndexOf(":");
      u = url.substring(0, pos + 1) + "sub/subTest";
    }

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(u);

    try {
      ODatabaseHelper.dropDatabase(db);
    } catch (OStorageException e) {
      Assert.assertTrue(e.getCause().getMessage().equals("Database with name 'sub/subTest' doesn't exits."));
    }
    ODatabaseHelper.createDatabase(db, u);

    db = ODatabaseDocumentPool.global().acquire(u, "admin", "admin");
    if (u.startsWith("remote:"))
      db.close();

    ODatabaseHelper.dropDatabase(db);
  }

  @Test
  public void testCreateAndConnectionPool() throws IOException {
    ODatabaseDocument db = new ODatabaseDocumentTx(url);

    ODatabaseHelper.dropDatabase(db);

    ODatabaseHelper.createDatabase(db, url);
    db.close();
    // Get connection from pool
    db = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
    db.close();

    // Destroy db in the back of the pool
    db = new ODatabaseDocumentTx(url);
    ODatabaseHelper.dropDatabase(db);

    // Re-create it so that the db exists for the pool
    db = new ODatabaseDocumentTx(url);
    ODatabaseHelper.createDatabase(db, url);
    db.close();

    ODatabaseDocumentPool.global().close();
  }

  @Test
  public void testOpenCloseConnectionPool() throws IOException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    if (!ODatabaseHelper.existsDatabase(db)) {
      ODatabaseHelper.createDatabase(db, url);
      db.close();
    }

    for (int i = 0; i < 500; i++) {
      ODatabaseDocumentPool.global().acquire(url, "admin", "admin").close();
    }
  }

  @Test(dependsOnMethods = { "testChangeLocale" })
  public void testSubFolderMultipleDbCreateSameName() throws IOException {
    int pos = url.lastIndexOf("/");
    String u = url;

    if (pos > -1)
      u = url.substring(0, pos);
    else {
      pos = url.lastIndexOf(":");
      u = url.substring(0, pos + 1);
    }

    for (int i = 0; i < 3; ++i) {
      String ur = u + "/" + i + "$db";
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(ur);

      try {
        ODatabaseHelper.dropDatabase(db);
      } catch (OStorageException e) {
        Assert.assertTrue(e.getCause().getMessage().contains("doesn't exits."));
      }
      ODatabaseHelper.createDatabase(db, ur);
      Assert.assertTrue(ODatabaseHelper.existsDatabase(db));
      db.open("admin", "admin");
    }

    for (int i = 0; i < 3; ++i) {
      String ur = u + "/" + i + "$db";
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(ur);
      Assert.assertTrue(ODatabaseHelper.existsDatabase(db));
      ODatabaseHelper.dropDatabase(db);
      Assert.assertFalse(ODatabaseHelper.existsDatabase(db));
    }

  }
}
