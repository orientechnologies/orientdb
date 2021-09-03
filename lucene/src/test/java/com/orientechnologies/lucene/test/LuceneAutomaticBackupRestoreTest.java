/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Created by Enrico Risa on 07/07/15. */
@RunWith(JUnit4.class)
public class LuceneAutomaticBackupRestoreTest {

  private static final String DBNAME = "OLuceneAutomaticBackupRestoreTest";
  private File tempFolder;

  @Rule public TestName name = new TestName();

  private OrientDB orientDB;
  private String URL = null;
  private String BACKUPDIR = null;
  private String BACKUFILE = null;

  private OServer server;
  private ODatabaseDocumentInternal db;

  @Before
  public void setUp() throws Exception {

    final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    Assume.assumeFalse(os.contains("win"));

    final String buildDirectory = System.getProperty("buildDirectory", "target");
    final File buildDirectoryFile = new File(buildDirectory);

    tempFolder = new File(buildDirectoryFile, name.getMethodName());
    OFileUtils.deleteRecursively(tempFolder);
    Assert.assertTrue(tempFolder.mkdirs());

    System.setProperty("ORIENTDB_HOME", tempFolder.getCanonicalPath());

    String path = tempFolder.getCanonicalPath() + File.separator + "databases";
    server =
        new OServer(false) {
          @Override
          public Map<String, String> getAvailableStorageNames() {
            HashMap<String, String> result = new HashMap<>();
            result.put(DBNAME, URL);
            return result;
          }
        };
    server.startup();

    orientDB = server.getContext();

    URL = "plocal:" + path + File.separator + DBNAME;

    BACKUPDIR = tempFolder.getCanonicalPath() + File.separator + "backups";

    BACKUFILE = BACKUPDIR + File.separator + DBNAME;

    File config = new File(tempFolder, "config");
    Assert.assertTrue(config.mkdirs());

    dropIfExists();

    orientDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin) ", DBNAME);

    db = (ODatabaseDocumentInternal) orientDB.open(DBNAME, "admin", "admin");

    db.command("create class City ");
    db.command("create property City.name string");
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    db.save(doc);
  }

  private void dropIfExists() {

    if (orientDB.exists(DBNAME)) {
      orientDB.drop(DBNAME);
    }
  }

  @After
  public void tearDown() throws Exception {
    final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    if (!os.contains("win")) {
      dropIfExists();

      OFileUtils.deleteRecursively(tempFolder);
    }
  }

  @AfterClass
  public static void afterClass() {
    final Orient orient = Orient.instance();

    if (orient != null) {
      orient.shutdown();
      orient.startup();
    }
  }

  @Test
  public void shouldBackupAndRestore() throws IOException, InterruptedException {
    try (OResultSet query = db.query("select from City where name lucene 'Rome'")) {
      assertThat(query).hasSize(1);
    }

    String jsonConfig =
        OIOUtils.readStreamAsString(
            getClass().getClassLoader().getResourceAsStream("automatic-backup.json"));

    ODocument doc = new ODocument().fromJSON(jsonConfig);

    doc.field("enabled", true);
    doc.field("targetFileName", "${DBNAME}.zip");

    doc.field("targetDirectory", BACKUPDIR);

    doc.field("dbInclude", new String[] {DBNAME});

    doc.field(
        "firstTime",
        new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000)));

    OIOUtils.writeFile(
        new File(tempFolder.getCanonicalPath(), "config/automatic-backup.json"), doc.toJSON());

    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {};

    aBackup.config(server, config);

    final CountDownLatch latch = new CountDownLatch(1);

    aBackup.registerListener(
        new OAutomaticBackup.OAutomaticBackupListener() {
          @Override
          public void onBackupCompleted(String database) {
            latch.countDown();
          }

          @Override
          public void onBackupError(String database, Exception e) {
            latch.countDown();
          }
        });

    latch.await();

    aBackup.sendShutdown();

    // RESTORE
    dropIfExists();

    db = createAndOpen();

    FileInputStream stream = new FileInputStream(new File(BACKUFILE + ".zip"));

    db.restore(stream, null, null, null);

    db.close();

    // VERIFY
    db = open();

    assertThat(db.countClass("City")).isEqualTo(1);

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(OClass.INDEX_TYPE.FULLTEXT.name());

    assertThat(db.query("select from City where name lucene 'Rome'")).hasSize(1);
  }

  @Test
  public void shouldExportImport() throws IOException, InterruptedException {

    try (OResultSet query = db.query("select from City where name lucene 'Rome'")) {
      assertThat(query).hasSize(1);
    }

    String jsonConfig =
        OIOUtils.readStreamAsString(
            getClass().getClassLoader().getResourceAsStream("automatic-backup.json"));

    ODocument doc = new ODocument().fromJSON(jsonConfig);

    doc.field("enabled", true);
    doc.field("targetFileName", "${DBNAME}.json");

    doc.field("targetDirectory", BACKUPDIR);
    doc.field("mode", "EXPORT");

    doc.field("dbInclude", new String[] {"OLuceneAutomaticBackupRestoreTest"});

    doc.field(
        "firstTime",
        new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000)));

    OIOUtils.writeFile(new File(tempFolder, "config/automatic-backup.json"), doc.toJSON());

    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {};

    aBackup.config(server, config);
    final CountDownLatch latch = new CountDownLatch(1);

    aBackup.registerListener(
        new OAutomaticBackup.OAutomaticBackupListener() {
          @Override
          public void onBackupCompleted(String database) {
            latch.countDown();
          }

          @Override
          public void onBackupError(String database, Exception e) {
            latch.countDown();
          }
        });
    latch.await();
    aBackup.sendShutdown();

    db.close();

    dropIfExists();
    // RESTORE

    db = createAndOpen();

    try (final GZIPInputStream stream =
        new GZIPInputStream(new FileInputStream(BACKUFILE + ".json.gz"))) {
      new ODatabaseImport(db, stream, s -> {}).importDatabase();
    }

    db.close();

    // VERIFY
    db = open();

    assertThat(db.countClass("City")).isEqualTo(1);

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(OClass.INDEX_TYPE.FULLTEXT.name());

    assertThat(db.query("select from City where name lucene 'Rome'")).hasSize(1);
  }

  private ODatabaseDocumentInternal createAndOpen() {
    orientDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin) ", DBNAME);
    return open();
  }

  private ODatabaseDocumentInternal open() {
    return (ODatabaseDocumentInternal) orientDB.open(DBNAME, "admin", "admin");
  }
}
