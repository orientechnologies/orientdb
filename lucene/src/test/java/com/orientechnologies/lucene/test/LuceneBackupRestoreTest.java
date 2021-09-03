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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Locale;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Created by Enrico Risa on 07/07/15. */
@RunWith(JUnit4.class)
public class LuceneBackupRestoreTest {

  @Rule public TestName name = new TestName();

  private File tempFolder;

  private OrientDB orientDB;

  private ODatabaseDocumentInternal databaseDocumentTx;

  @Before
  public void setUp() throws Exception {
    final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);

    Assume.assumeFalse(os.contains("win"));

    final String buildDirectory = System.getProperty("buildDirectory", "target");
    final File buildDirectoryFile = new File(buildDirectory);

    tempFolder = new File(buildDirectoryFile, name.getMethodName());

    orientDB =
        new OrientDB("plocal:" + tempFolder.getCanonicalPath(), OrientDBConfig.defaultConfig());

    dropIfExists();

    final String dbName = getClass().getSimpleName();

    orientDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", dbName);
    databaseDocumentTx = (ODatabaseDocumentInternal) orientDB.open(dbName, "admin", "admin");

    databaseDocumentTx.command("create class City ");
    databaseDocumentTx.command("create property City.name string");
    databaseDocumentTx.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    databaseDocumentTx.save(doc);
  }

  private void dropIfExists() {
    final String dbName = getClass().getSimpleName();

    if (orientDB.exists(dbName)) {
      orientDB.drop(dbName);
    }
  }

  @After
  public void tearDown() throws Exception {
    final String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    if (!os.contains("win")) dropIfExists();
  }

  @Test
  public void shouldBackupAndRestore() throws IOException {
    File backupFile = new File(tempFolder, "backupRestore.gz");

    try (OResultSet query = databaseDocumentTx.query("select from City where name lucene 'Rome'")) {
      assertThat(query).hasSize(1);
    }

    databaseDocumentTx.backup(new FileOutputStream(backupFile), null, null, null, 9, 1048576);

    orientDB.drop(getClass().getSimpleName());
    orientDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)",
        getClass().getSimpleName());
    databaseDocumentTx =
        (ODatabaseDocumentInternal) orientDB.open(getClass().getSimpleName(), "admin", "admin");

    FileInputStream stream = new FileInputStream(backupFile);

    databaseDocumentTx.restore(stream, null, null, null);

    assertThat(databaseDocumentTx.countClass("City")).isEqualTo(1);

    OIndex index =
        databaseDocumentTx
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(databaseDocumentTx, "City.name");

    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(OClass.INDEX_TYPE.FULLTEXT.name());

    try (OResultSet query = databaseDocumentTx.query("select from City where name lucene 'Rome'")) {
      assertThat(query).hasSize(1);
    }
  }
}
