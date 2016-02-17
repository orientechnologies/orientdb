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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/9/2015
 */

public class StorageBackupTest {
  private String buildDirectory;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".");

    //    OEngineLocalPaginated plocalEngine = (OEngineLocalPaginated) Orient.instance().getEngine("plocal");
    //    Orient.instance().registerEngine(new OEnterpriseEnginePaginated(plocalEngine));
  }

  @Test
  public void testSingeThreadFullBackup() throws IOException {
    final String dbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    db.create();

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("data", OType.BINARY);

    backupClass.createIndex("backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final ODocument document = new ODocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
    }

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    db.incrementalBackup(backupDir.getAbsolutePath());
    final OStorage storage = db.getStorage();
    db.close();

    storage.close(true, false);

    final String backedUpDbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName() + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create();

    backedUpDb.incrementalRestore(backupDir.getAbsolutePath());
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

    db.open("admin", "admin");
    db.drop();

    backedUpDb.open("admin", "admin");
    backedUpDb.drop();

    OFileUtils.deleteRecursively(backupDir);
  }

  @Test
  public void testSingeThreadIncrementalBackup() throws IOException {
    final String dbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    db.create();

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("data", OType.BINARY);

    backupClass.createIndex("backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final ODocument document = new ODocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
    }

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    db.incrementalBackup(backupDir.getAbsolutePath());

    for (int n = 0; n < 3; n++) {
      for (int i = 0; i < 1000; i++) {
        final byte[] data = new byte[16];
        random.nextBytes(data);

        final int num = random.nextInt();

        final ODocument document = new ODocument("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
      }

      db.incrementalBackup(backupDir.getAbsolutePath());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());

    final OStorage storage = db.getStorage();
    db.close();

    storage.close(true, false);

    final String backedUpDbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName() + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create();

    backedUpDb.incrementalRestore(backupDir.getAbsolutePath());
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

     db.open("admin", "admin");
     db.drop();

     backedUpDb.open("admin", "admin");
     backedUpDb.drop();

     OFileUtils.deleteRecursively(backupDir);
  }

}