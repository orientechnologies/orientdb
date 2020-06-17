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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 23/09/14. */
public class OLuceneMassiveInsertDeleteTest extends OLuceneBaseTest {

  @Before
  public void init() {
    OSchema schema = db.getMetadata().getSchema();
    OClass song = db.createVertexClass("City");
    song.createProperty("name", OType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void loadCloseDelete() {

    int size = 1000;
    for (int i = 0; i < size; i++) {
      OVertex city = db.newVertex("City");
      city.setProperty("name", "Rome " + i);
      db.save(city);
    }
    String query = "select * from City where search_class('name:Rome')=true";
    OResultSet docs = db.query(query);
    Assertions.assertThat(docs).hasSize(size);
    docs.close();
    db.close();

    db = (ODatabaseDocumentInternal) pool.acquire();
    docs = db.query(query);
    Assertions.assertThat(docs).hasSize(size);
    docs.close();
    db.command("delete vertex City");

    docs = db.query(query);
    Assertions.assertThat(docs).hasSize(0);
    docs.close();
    db.close();
    db = (ODatabaseDocumentInternal) pool.acquire();
    docs = db.query(query);
    Assertions.assertThat(docs).hasSize(0);
    docs.close();
    db.getMetadata().reload();
    OIndex idx = db.getMetadata().getSchema().getClass("City").getClassIndex("City.name");
    Assert.assertEquals(0, idx.getInternal().size());
  }
}
