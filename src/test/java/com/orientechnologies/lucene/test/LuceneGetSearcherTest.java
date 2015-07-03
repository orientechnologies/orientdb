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

import com.orientechnologies.lucene.index.OLuceneIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;

/**
 * Created by Enrico Risa on 29/04/15.
 */
public class LuceneGetSearcherTest extends BaseLuceneTest {
  @Override
  protected String getDatabaseName() {
    return "getSearcher";
  }

  @BeforeClass
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass song = schema.createClass("Person");
    song.setSuperClass(v);
    song.createProperty("isDeleted", OType.BOOLEAN);

    databaseDocumentTx.command(new OCommandSQL("create index Person.isDeleted on Person (isDeleted) FULLTEXT ENGINE LUCENE"))
        .execute();

  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  public void testSearcherInstance() {

    OIndex<?> index = databaseDocumentTx.getMetadata().getIndexManager().getIndex("Person.isDeleted");

    Assert.assertEquals(true, index.getInternal() instanceof OLuceneIndexNotUnique);

    OLuceneIndexNotUnique idx = (OLuceneIndexNotUnique) index.getInternal();

    try {
      Assert.assertNotNull(idx.searcher());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
