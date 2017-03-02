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

package com.orientechnologies.lucene.tests;

import com.orientechnologies.lucene.OLuceneIndexFactory;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE.FULLTEXT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 07/07/15.
 */
public class OLuceneExportImportTest extends OLuceneBaseTest {

  @Before
  public void init() {

    OSchema schema = db.getMetadata().getSchema();
    OClass city = schema.createClass("City");
    city.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE")).execute();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    db.save(doc);
  }

  @Test
  public void testExportImport() {

    String file = "./target/exportTest.json";

    OResultSet query = db.query("select from City where search_class('Rome')=true");

    assertThat(query).hasSize(1);

    query.close();
    try {

      //export
      new ODatabaseExport(db, file, new OCommandOutputListener() {
        @Override
        public void onMessage(String s) {
        }
      }).exportDatabase();

      //import
      db.drop();
      db.create();
      GZIPInputStream stream = new GZIPInputStream(new FileInputStream(file + ".gz"));
      new ODatabaseImport(db, stream, new OCommandOutputListener() {
        @Override
        public void onMessage(String s) {
        }
      }).importDatabase();
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

    assertThat(db.countClass("City")).isEqualTo(1);
    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("City.name");

    assertThat(index.getType()).isEqualTo(FULLTEXT.toString());

    assertThat(index.getAlgorithm()).isEqualTo(OLuceneIndexFactory.LUCENE_ALGORITHM);

    //redo the query
    query = db.query("select from City where search_class('Rome')=true");

    assertThat(query).hasSize(1);
    query.close();
  }

}
