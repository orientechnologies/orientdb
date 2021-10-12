/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

// FIXME: let exporter version exports be 13 and check whether new stream processing is used.
@Test(groups = {"db", "import-export"})
public class DbImportStreamExportTest extends DocumentDBBaseTest implements OCommandOutputListener {
  public static final String EXPORT_FILE_PATH = "target/db.export.gz";
  public static final String NEW_DB_PATH = "target/test-import";
  public static final String NEW_DB_URL = "target/test-import";

  private final String testPath;
  private final String exportFilePath;
  private boolean dumpMode = false;

  @Parameters(value = {"url", "testPath"})
  public DbImportStreamExportTest(@Optional String url, String testPath) {
    super(url);
    this.testPath = testPath;
    this.exportFilePath = System.getProperty("exportFilePath", EXPORT_FILE_PATH);
  }

  @Test
  public void testDbExport() throws IOException {
    final ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    // ADD A CUSTOM TO THE CLASS
    database
        .command(new OCommandSQL("alter class V custom onBeforeCreate=onBeforeCreateItem"))
        .execute();

    final ODatabaseExport export =
        new ODatabaseExport(database, testPath + "/" + exportFilePath, this);
    export.exportDatabase();
    export.close();
    database.close();
  }

  @Test(dependsOnMethods = "testDbExport")
  public void testDbImport() throws IOException {
    final File importDir = new File(testPath + "/" + NEW_DB_PATH);
    if (importDir.exists()) {
      for (final File f : importDir.listFiles()) {
        f.delete();
      }
    } else {
      importDir.mkdir();
    }

    final ODatabaseDocumentTx database =
        new ODatabaseDocumentTx(getStorageType() + ":" + testPath + "/" + NEW_DB_URL);
    database.create();

    final ODatabaseImport dbImport =
        new ODatabaseImport(database, new FileInputStream(importDir), this);

    // UNREGISTER ALL THE HOOKS
    for (final ORecordHook hook : new ArrayList<>(database.getHooks().keySet())) {
      database.unregisterHook(hook);
    }
    dbImport.setPreserveRids(true);
    dbImport.setDeleteRIDMapping(false);
    dbImport.importDatabase();
    dbImport.close();

    database.close();
  }

  @Test(dependsOnMethods = "testDbImport")
  public void testCompareDatabases() throws IOException {
    if ("remote".equals(getStorageType()) || url.startsWith("remote:")) {
      final String env = getTestEnv();
      if (env == null || env.equals("dev")) {
        return;
      }
      // EXECUTES ONLY IF NOT REMOTE ON CI/RELEASE TEST ENV
    }
    final String urlPrefix = getStorageType() + ":";

    final ODatabaseCompare databaseCompare =
        new ODatabaseCompare(
            url,
            urlPrefix + testPath + "/" + DbImportStreamExportTest.NEW_DB_URL,
            "admin",
            "admin",
            this);
    databaseCompare.setCompareEntriesForAutomaticIndexes(true);
    databaseCompare.setCompareIndexMetadata(true);

    Assert.assertTrue(databaseCompare.compare());
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    if (iText != null && iText.contains("ERR")) {
      // ACTIVATE DUMP MODE
      dumpMode = true;
    }
    if (dumpMode) {
      OLogManager.instance().error(this, iText, null);
    }
  }
}
