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

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.hook.ORecordHook;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

@Test(groups = { "db", "import-export" })
public class DbImportExportTest extends DocumentDBBaseTest implements OCommandOutputListener {
  public static final String EXPORT_FILE_PATH                = "target/db.export.gz";
  public static final String EXPORT_FILE_NO_COMPRESSION_PATH = "target/db.export.json";
  public static final String EXPORT_NO_COMPRESSED_FILE_PATH  = "target/db.export-no-compressed.gz";
  public static final String NEW_DB_PATH                     = "target/test-import";
  public static final String NEW_DB_URL                      = "target/test-import";
  public static final String NEW_DB_URL_COMPRESSED           = "target/test-import-compressed";
  public static final String NEW_DB_URL_NO_COMPRESSED        = "target/test-import-no-compressed";

  private String             testPath;

  @Parameters(value = { "url", "testPath" })
  public DbImportExportTest(@Optional String url, String testPath) {
    super(url);
    this.testPath = testPath;
  }

  @Test
  public void testDbExport() throws IOException {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODatabaseExport export = new ODatabaseExport(database, testPath + "/" + EXPORT_FILE_PATH, this);
    export.exportDatabase();
    export.close();

    database.close();
  }

  @Test(dependsOnMethods = "testDbExport")
  public void testDbImport() throws IOException {
    final File importDir = new File(testPath + "/" + NEW_DB_PATH);
    if (importDir.exists())
      for (File f : importDir.listFiles())
        f.delete();
    else
      importDir.mkdir();

    ODatabaseDocumentTx database = new ODatabaseDocumentTx(getStorageType() + ":" + testPath + "/" + NEW_DB_URL);
    database.create();

    ODatabaseImport dbImport = new ODatabaseImport(database, testPath + "/" + EXPORT_FILE_PATH, this);

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
  public void testDbExportSize() throws IOException {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODatabaseExport export = new ODatabaseExport(database, testPath + "/" + EXPORT_FILE_PATH, this)
        .setOptions("-compressionLevel=9");

    export.exportDatabase();
    export.close();

    export = new ODatabaseExport(database, testPath + "/" + EXPORT_NO_COMPRESSED_FILE_PATH, this).setOptions("-compressionLevel=0");
    export.exportDatabase();
    export.close();

    database.close();

    File file1 = new File(testPath + "/" + EXPORT_FILE_PATH);
    File file2 = new File(testPath + "/" + EXPORT_NO_COMPRESSED_FILE_PATH);

    long file1Size = file1.length();
    long file2Size = file2.length();

    Assert.assertEquals(true, file2Size > file1Size);

    // try to import the compressed one with 9

    database = new ODatabaseDocumentTx(getStorageType() + ":" + testPath + "/" + NEW_DB_URL_COMPRESSED);
    if (database.exists())
      database.open("admin", "admin").drop();
    database.create();

    ODatabaseImport dbImport = new ODatabaseImport(database, testPath + "/" + EXPORT_FILE_PATH, this);

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
  public void testDbExportNoCompression() throws IOException {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODatabaseExport export = new ODatabaseExport(database, testPath + "/" + EXPORT_FILE_NO_COMPRESSION_PATH, this)
        .setOptions("-noCompression");

    export.exportDatabase();
    export.close();

    database.close();

    // try to import the compressed one with 9

    database = new ODatabaseDocumentTx(getStorageType() + ":" + testPath + "/" + NEW_DB_URL_NO_COMPRESSED);
    if (database.exists())
      database.open("admin", "admin").drop();
    database.create();

    ODatabaseImport dbImport = new ODatabaseImport(database, testPath + "/" + EXPORT_FILE_NO_COMPRESSION_PATH, this);

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

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    // System.out.print(iText);
    // System.out.flush();
  }
}
