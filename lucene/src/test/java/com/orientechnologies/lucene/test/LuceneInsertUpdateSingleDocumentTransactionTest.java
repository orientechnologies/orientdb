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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 28/06/14. */
public class LuceneInsertUpdateSingleDocumentTransactionTest extends BaseLuceneTest {

  public LuceneInsertUpdateSingleDocumentTransactionTest() {
    super();
  }

  @Before
  public void init() {
    OSchema schema = db.getMetadata().getSchema();

    OClass oClass = schema.createClass("City");
    oClass.createProperty("name", OType.STRING);
    //noinspection deprecation
    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE"))
        .execute();
  }

  @Test
  public void testInsertUpdateTransactionWithIndex() {

    db.close();
    db = (ODatabaseDocumentInternal) openDatabase();
    OSchema schema = db.getMetadata().getSchema();
    schema.reload();
    db.begin();
    ODocument doc = new ODocument("City");
    doc.field("name", "");
    ODocument doc1 = new ODocument("City");
    doc1.field("name", "");
    doc = db.save(doc);
    doc1 = db.save(doc1);
    db.commit();
    db.begin();
    doc = db.load(doc);
    doc1 = db.load(doc1);
    doc.field("name", "Rome");
    doc1.field("name", "Rome");
    db.save(doc);
    db.save(doc1);
    db.commit();
    OIndex idx = schema.getClass("City").getClassIndex("City.name");
    Collection<?> coll;
    try (Stream<ORID> stream = idx.getInternal().getRids("Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 2);
    Assert.assertEquals(2, idx.getInternal().size());
  }
}
