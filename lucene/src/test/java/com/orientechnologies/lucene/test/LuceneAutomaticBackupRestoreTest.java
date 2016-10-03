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

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 07/07/15.
 */
public class LuceneAutomaticBackupRestoreTest {

  @Rule
  public TemporaryFolder      tempFolder = new TemporaryFolder();

  private final static String DBNAME     = "LuceneAutomaticBackupRestoreTest";
  private String              URL        = null;
  private String              BACKUPDIR  = null;
  private String              BACKUFILE  = null;

  private OServer             server;
  private ODatabaseDocumentTx databaseDocumentTx;

  @Before
  public void setUp() throws Exception {

    System.setProperty("ORIENTDB_HOME", tempFolder.getRoot().getAbsolutePath());

    URL = "plocal:" + tempFolder.getRoot().getAbsolutePath() + File.separator + "databases" + File.separator + DBNAME;

    BACKUPDIR = tempFolder.getRoot().getAbsolutePath() + File.separator + "backups";

    BACKUFILE = BACKUPDIR + File.separator + DBNAME;

    tempFolder.newFolder("config");

    server = new OServer() {
      @Override
      public Map<String, String> getAvailableStorageNames() {
        HashMap<String, String> result = new HashMap<String, String>();
        result.put(DBNAME, URL);
        return result;
      }
    };

    databaseDocumentTx = new ODatabaseDocumentTx(URL);

    dropIfExists();

    databaseDocumentTx.create();

    databaseDocumentTx.command(new OCommandSQL("create class City ")).execute();
    databaseDocumentTx.command(new OCommandSQL("create property City.name string")).execute();
    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    databaseDocumentTx.save(doc);
  }

  private void dropIfExists() {
    if (databaseDocumentTx.exists()) {
      if (databaseDocumentTx.isClosed())
        databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }
  }

  @After
  public void tearDown() throws Exception {
    dropIfExists();

    tempFolder.delete();

  }

  @Test
  public void shouldBackupAndRestore() throws IOException, InterruptedException {

    List<?> query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));

    Assert.assertEquals(query.size(), 1);

    String jsonConfig = OIOUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("automatic-backup.json"));

    ODocument doc = new ODocument().fromJSON(jsonConfig);

    doc.field("enabled", true);
    doc.field("targetFileName", "${DBNAME}.zip");

    doc.field("targetDirectory", BACKUPDIR);

    doc.field("dbInclude", new String[] { "LuceneAutomaticBackupRestoreTest" });

    doc.field("firstTime", new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000)));

    OIOUtils.writeFile(new File(tempFolder.getRoot().getAbsolutePath() + "/config/automatic-backup.json"), doc.toJSON());

    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {};

    aBackup.config(server, config);

    final CountDownLatch latch = new CountDownLatch(1);

    aBackup.registerListener(new OAutomaticBackup.OAutomaticBackupListener() {
      @Override
      public void onBackupCompleted(String database) {
        latch.countDown();
      }
    });

    latch.await();

    aBackup.sendShutdown();

    // RESTORE
    databaseDocumentTx.drop();

    databaseDocumentTx.create();

    FileInputStream stream = new FileInputStream(new File(BACKUFILE + ".zip"));

    databaseDocumentTx.restore(stream, null, null, null);

    databaseDocumentTx.close();

    // VERIFY
    databaseDocumentTx.open("admin", "admin");

    assertThat(databaseDocumentTx.countClass("City")).isEqualTo(1);

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(OClass.INDEX_TYPE.FULLTEXT.name());

    assertThat((List<?>) databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"))).hasSize(1);
  }

  @Test
  public void shouldExportImport() throws IOException, InterruptedException {

    List<?> query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));

    Assert.assertEquals(query.size(), 1);

    String jsonConfig = OIOUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("automatic-backup.json"));

    ODocument doc = new ODocument().fromJSON(jsonConfig);

    doc.field("enabled", true);
    doc.field("targetFileName", "${DBNAME}.json");

    doc.field("targetDirectory", BACKUPDIR);
    doc.field("mode", "EXPORT");

    doc.field("dbInclude", new String[] { "LuceneAutomaticBackupRestoreTest" });

    doc.field("firstTime", new SimpleDateFormat("HH:mm:ss").format(new Date(System.currentTimeMillis() + 2000)));

    OIOUtils.writeFile(new File(tempFolder.getRoot().getAbsolutePath() + "/config/automatic-backup.json"), doc.toJSON());

    final OAutomaticBackup aBackup = new OAutomaticBackup();

    final OServerParameterConfiguration[] config = new OServerParameterConfiguration[] {};

    aBackup.config(server, config);
    final CountDownLatch latch = new CountDownLatch(1);

    aBackup.registerListener(new OAutomaticBackup.OAutomaticBackupListener() {
      @Override
      public void onBackupCompleted(String database) {
        latch.countDown();
      }
    });
    latch.await();
    aBackup.sendShutdown();

    // RESTORE
    databaseDocumentTx.drop();

    databaseDocumentTx.create();

    GZIPInputStream stream = new GZIPInputStream(new FileInputStream(BACKUFILE + ".json.gz"));
    new ODatabaseImport(databaseDocumentTx, stream, new OCommandOutputListener() {
      @Override
      public void onMessage(String s) {
      }
    }).importDatabase();

    databaseDocumentTx.close();

    // VERIFY
    databaseDocumentTx.open("admin", "admin");

    assertThat(databaseDocumentTx.countClass("City")).isEqualTo(1);

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(OClass.INDEX_TYPE.FULLTEXT.name());

    assertThat((List<?>) databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"))).hasSize(1);
  }

}
