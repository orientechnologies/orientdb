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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.List;

/**
 * Created by Enrico Risa on 07/07/15.
 */
public class LuceneBackupRestoreTest {

  @Test
  public void testExportImport() {

    String property = "java.io.tmpdir";

    String file = System.getProperty(property) + "backupRestore.gz";

    String buildDirectory = System.getProperty("buildDirectory", ".");
    String url = OEngineLocalPaginated.NAME + ":" + buildDirectory + "/databases/restoreTest";

    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(url);
    try {
      if (databaseDocumentTx.exists()) {
        databaseDocumentTx.open("admin", "admin");
        databaseDocumentTx.drop();
      }
      databaseDocumentTx.create();

      OSchema schema = databaseDocumentTx.getMetadata().getSchema();
      OClass oClass = schema.createClass("City");

      oClass.createProperty("name", OType.STRING);
      databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

      ODocument doc = new ODocument("City");
      doc.field("name", "Rome");
      databaseDocumentTx.save(doc);

      List<?> query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));

      Assert.assertEquals(query.size(), 1);

      databaseDocumentTx.backup(new FileOutputStream(new File(file)), null, null, null, 9, 1048576);

      databaseDocumentTx.drop();

      databaseDocumentTx.create();
      FileInputStream stream = new FileInputStream(file);

      databaseDocumentTx.restore(stream, null, null, null);

      databaseDocumentTx.close();
      databaseDocumentTx.open("admin", "admin");
      long city = databaseDocumentTx.countClass("City");

      Assert.assertEquals(city, 1);

      OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("City.name");

      Assert.assertNotNull(index);
      Assert.assertEquals(index.getType(), "FULLTEXT");

      query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));
      Assert.assertEquals(query.size(), 1);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      databaseDocumentTx.drop();
      File f = new File(file);
      f.delete();
    }

  }

}
