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

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

@Test(groups = "db")
public class DbDeleteTest {
  private String testPath;
  private String url;

  @Parameters(value = { "url", "testPath" })
  public DbDeleteTest(String iURL, String iTestPath) {
    testPath = iTestPath;
    url = iURL;
    Orient.instance().getProfiler().startRecording();
  }

  public void testDbDeleteNoCredential() throws IOException {
    ODatabaseDocument db = new ODatabaseDocumentTx(url);
    try {
      db.drop();
      Assert.fail("Should have thrown ODatabaseException because trying to delete a not opened");
    } catch (ODatabaseException e) {
      Assert.assertTrue(e.getMessage().equals("Database '" + url + "' is closed"));
    } catch (OStorageException e) {
      Assert.assertTrue(e.getMessage().startsWith("Cannot delete the remote storage:"));
    }
  }

  @Test(dependsOnMethods = { "testDbDeleteNoCredential" })
  public void testDbDelete() throws IOException {
    String prefix = url.substring(0, url.indexOf(':') + 1);
    if (prefix.equals("memory:") || prefix.equals("remote:"))
      return;

    ODatabaseDocument db = new ODatabaseDocumentTx(prefix + testPath + "/" + DbImportExportTest.NEW_DB_URL);
    if (!db.exists())
      db.create();

    if (db.exists()) {
      if (db.isClosed())
        db.open("admin", "admin");

      removeExportImportMapping(db);
    }

    ODatabaseHelper.dropDatabase(db);

    Assert.assertFalse(new File(testPath + "/" + DbImportExportTest.NEW_DB_PATH).exists());
  }

  public void testDbDeleteWithIndex() {
    String prefix = url.substring(0, url.indexOf(':') + 1);
    if (prefix.equals("memory:") || prefix.equals("remote:"))
      return;

    ODatabaseDocument db = new ODatabaseDocumentTx(prefix + testPath + "/" + DbImportExportTest.NEW_DB_URL);
    if (!db.exists())
      db.create();

    if (db.exists()) {
      if (db.isClosed())
        db.open("admin", "admin");

      removeExportImportMapping(db);
      db.drop();
      db.create();
    }

    final OClass indexedClass = db.getMetadata().getSchema().createClass("IndexedClass");
    indexedClass.createProperty("value", OType.STRING);
    indexedClass.createIndex("indexValue", OClass.INDEX_TYPE.UNIQUE, "value");

    final ODocument document = new ODocument("IndexedClass");
    document.field("value", "value");
    document.save();

    db.drop();
  }

  private void removeExportImportMapping(ODatabaseDocument databaseDocument) {
    File file = new File(databaseDocument.getStorage().getConfiguration().getDirectory() + File.separator
        + ODatabaseImport.EXPORT_IMPORT_MAP_NAME + ODatabaseImport.EXPORT_IMPORT_MAP_TREE_STATE_EXT);
    if (file.exists()) {
      OMurmurHash3HashFunction<OIdentifiable> keyHashFunction = new OMurmurHash3HashFunction<OIdentifiable>();
      keyHashFunction.setValueSerializer(OLinkSerializer.INSTANCE);

      OLocalHashTable<OIdentifiable, OIdentifiable> exportImportHashTable = new OLocalHashTable<OIdentifiable, OIdentifiable>(
          ODatabaseImport.EXPORT_IMPORT_MAP_METADATA_EXT, ODatabaseImport.EXPORT_IMPORT_MAP_TREE_STATE_EXT,
          ODatabaseImport.EXPORT_IMPORT_MAP_BF_EXT, keyHashFunction);
      exportImportHashTable.load(ODatabaseImport.EXPORT_IMPORT_MAP_NAME, (OStorageLocalAbstract) databaseDocument.getStorage());
      exportImportHashTable.delete();
    }
  }
}
