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
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.hook.ORecordHook;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"db", "import-export"})
public class DbImportExportRidbagTest extends DocumentDBBaseTest implements OCommandOutputListener {
  private static final OLogger logger =
      OLogManager.instance().logger(DbImportExportRidbagTest.class);
  public static final String EXPORT_FILE_PATH = "target/db.export-ridbag.gz";
  public static final String NEW_DB_PATH = "target/test-import-ridbag";
  public static final String NEW_DB_URL = "target/test-import-ridbag";

  private String testPath;
  private String exportFilePath;
  private boolean dumpMode = false;

  @Parameters(value = {"url", "testPath"})
  public DbImportExportRidbagTest(@Optional String url, String testPath) {
    super(url);
    this.testPath = testPath;

    exportFilePath = System.getProperty("exportFilePath", EXPORT_FILE_PATH);
  }

  @Test
  public void testDbExport() throws IOException {
    ODatabaseDocumentInternal database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    database.command("insert into V set name ='a'");
    for (int i = 0; i < 100; i++) {
      database.command("insert into V set name ='b" + i + "'");
    }

    database.command(
        "create edge E from (select from V where name ='a') to (select from V where name != 'a')");

    // ADD A CUSTOM TO THE CLASS
    database.command("alter class V custom onBeforeCreate=onBeforeCreateItem").close();

    ODatabaseExport export = new ODatabaseExport(database, testPath + "/" + exportFilePath, this);
    export.exportDatabase();
    export.close();

    database.close();
  }

  @Test(dependsOnMethods = "testDbExport")
  public void testDbImport() throws IOException {
    final File importDir = new File(testPath + "/" + NEW_DB_PATH);
    if (importDir.exists()) for (File f : importDir.listFiles()) f.delete();
    else importDir.mkdir();

    ODatabaseDocumentInternal database =
        new ODatabaseDocumentTx(getStorageType() + ":" + testPath + "/" + NEW_DB_URL);
    database.create();

    ODatabaseImport dbImport = new ODatabaseImport(database, testPath + "/" + exportFilePath, this);
    dbImport.setMaxRidbagStringSizeBeforeLazyImport(50);

    // UNREGISTER ALL THE HOOKS
    for (ORecordHook hook : new ArrayList<ORecordHook>(database.getHooks().keySet())) {
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
      String env = getTestEnv();
      if (env == null || env.equals("dev")) return;

      // EXECUTES ONLY IF NOT REMOTE ON CI/RELEASE TEST ENV
    }

    ODatabaseDocumentInternal first = new ODatabaseDocumentTx(url);
    first.open("admin", "admin");
    ODatabaseDocumentInternal second =
        new ODatabaseDocumentTx(getStorageType() + ":" + testPath + "/" + NEW_DB_URL);
    second.open("admin", "admin");

    final ODatabaseCompare databaseCompare = new ODatabaseCompare(first, second, this);
    databaseCompare.setCompareEntriesForAutomaticIndexes(true);
    databaseCompare.setCompareIndexMetadata(true);
    Assert.assertTrue(databaseCompare.compare());
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    if (iText != null && iText.contains("ERR"))
      // ACTIVATE DUMP MODE
      dumpMode = true;

    if (dumpMode) logger.error(iText, null);
  }
}
