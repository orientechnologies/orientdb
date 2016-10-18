/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

/**
 * Disabled for now. Re-enable it when this is implemented https://github.com/orientechnologies/orientdb/issues/5958
 * 
 * @author Enrico Risa <e.risa@orientdb.com>.
 * @since 4/4/2016
 */

public class StorageBackupTestWithLuceneIndex {
  private String              buildDirectory;

  private ODatabaseDocumentTx db;
  private String              dbDirectory;
  private String              backedUpDbDirectory;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".");
    dbDirectory = buildDirectory + File.separator + StorageBackupTestWithLuceneIndex.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));
    db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    db.create();

    backedUpDbDirectory = buildDirectory + File.separator + StorageBackupTestWithLuceneIndex.class.getSimpleName() + "BackUp";
  }

  @After
  public void after() {
    if (db.exists()) {
      if (db.isClosed()) {
        db.open("admin", "admin");
      }
      db.drop();
    }

    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    if (backedUpDb.exists()) {
      if (backedUpDb.isClosed()) {
        backedUpDb.open("admin", "admin");
        backedUpDb.drop();
      }
    }

    OFileUtils.deleteRecursively(new File(dbDirectory));
    OFileUtils.deleteRecursively(new File(buildDirectory, "backupDir"));

  }

  // @Test
  public void testSingeThreadFullBackup() throws IOException {

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("name", OType.STRING);

    backupClass.createIndex("backupLuceneIndex", OClass.INDEX_TYPE.FULLTEXT.toString(), null, null, "LUCENE",
        new String[] { "name" });

    final ODocument document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    document.save();

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    db.incrementalBackup(backupDir.getAbsolutePath());
    final OStorage storage = db.getStorage();
    db.close();

    storage.close(true, false);

    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(true, false);

    final ODatabaseCompare compare = new ODatabaseCompare("plocal:" + dbDirectory, "plocal:" + backedUpDbDirectory, "admin",
        "admin", new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            System.out.println(iText);
          }
        });

    Assert.assertTrue(compare.compare());

  }

  // @Test
  public void testSingeThreadIncrementalBackup() throws IOException {

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("name", OType.STRING);

    backupClass.createIndex("backupLuceneIndex", OClass.INDEX_TYPE.FULLTEXT.toString(), null, null, "LUCENE",
        new String[] { "name" });

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    ODocument document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage");
    document.save();

    db.incrementalBackup(backupDir.getAbsolutePath());

    document = new ODocument("BackupClass");
    document.field("num", 1);
    document.field("name", "Storage1");
    document.save();

    db.incrementalBackup(backupDir.getAbsolutePath());

    final OStorage storage = db.getStorage();
    db.close();

    storage.close(true, false);

    final String backedUpDbDirectory = buildDirectory + File.separator + StorageBackupTestWithLuceneIndex.class.getSimpleName()
        + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(true, false);

    final ODatabaseCompare compare = new ODatabaseCompare("plocal:" + dbDirectory, "plocal:" + backedUpDbDirectory, "admin",
        "admin", new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            System.out.println(iText);
          }
        });

    Assert.assertTrue(compare.compare());

  }

}