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

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by Enrico Risa on 07/07/15.
 */
public class LuceneExportImportTest extends BaseLuceneTest {

  @Override
  protected String getDatabaseName() {
    return "importExport";
  }

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

    String file = System.getProperty(property) + "test.json";

    List<?> query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));

    Assert.assertEquals(query.size(), 1);
    try {
      new ODatabaseExport(databaseDocumentTx, file, new OCommandOutputListener() {
        @Override
        public void onMessage(String s) {
        }
      }).exportDatabase();
      databaseDocumentTx.drop();
      databaseDocumentTx.create();
      GZIPInputStream stream = new GZIPInputStream(new FileInputStream(file + ".gz"));
      new ODatabaseImport(databaseDocumentTx, stream, new OCommandOutputListener() {
        @Override
        public void onMessage(String s) {
        }
      }).importDatabase();
    } catch (IOException e) {
      e.printStackTrace();
    }
    long city = databaseDocumentTx.countClass("City");

    Assert.assertEquals(city, 1);

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("City.name");

    Assert.assertNotNull(index);
    Assert.assertEquals(index.getType(), "FULLTEXT");
//    Assert.assertEquals(index.getAlgorithm(), "LUCENE");

    query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from City where name lucene 'Rome'"));
    Assert.assertEquals(query.size(), 1);
  }

  @AfterClass
  public void deInit() {

  }
}
