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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by Enrico Risa on 07/07/15.
 */
public class LuceneBackupRestoreTest extends BaseLuceneTest {



  @BeforeClass
  public void init() {
    initDB();

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    oClass.createProperty("name", OType.STRING);
    databaseDocumentTx.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    databaseDocumentTx.save(doc);
  }

  @Test
  public void testExportImport() {

    String property = "java.io.tmpdir";

    String file = System.getProperty(property) + "backupRestore.gz";

    List<?> query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));

    Assert.assertEquals(query.size(), 1);

    try {

      databaseDocumentTx.backup(new FileOutputStream(new File(file)), null, null, null, 9, 1048576);

      databaseDocumentTx.drop();

      databaseDocumentTx.create();
      FileInputStream stream = new FileInputStream(file);

      databaseDocumentTx.restore(stream, null, null, null);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      File f = new File(file);
      f.delete();
    }

    databaseDocumentTx.close();
    databaseDocumentTx.open("admin", "admin");
    long city = databaseDocumentTx.countClass("City");

    Assert.assertEquals(city, 1);

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("City.name");

    Assert.assertNotNull(index);
    Assert.assertEquals(index.getType(), "FULLTEXT");

    query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));
    Assert.assertEquals(query.size(), 1);

  }

  @AfterClass
  public void deInit() {

  }
}
