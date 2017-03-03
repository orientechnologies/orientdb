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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

/**
 * Created by enricorisa on 28/06/14.
 */

public class OLuceneInsertUpdateTest extends OLuceneBaseTest {

  @Before
  public void init() {

    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    oClass.createProperty("name", OType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testInsertUpdateWithIndex() throws Exception {

    OSchema schema = db.getMetadata().getSchema();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");

    db.save(doc);
    OIndex idx = schema.getClass("City").getClassIndex("City.name");
    Collection<?> coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 1);

    OIdentifiable next = (OIdentifiable) coll.iterator().next();
    doc = db.load(next.<ORecord>getRecord());
    Assert.assertEquals(doc.field("name"), "Rome");

    doc.field("name", "London");
    db.save(doc);

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 1);

    next = (OIdentifiable) coll.iterator().next();
    doc = db.load(next.<ORecord>getRecord());
    Assert.assertEquals(doc.field("name"), "London");

    doc.field("name", "Berlin");
    db.save(doc);

    coll = (Collection<?>) idx.get("Rome");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("London");
    Assert.assertEquals(coll.size(), 0);
    coll = (Collection<?>) idx.get("Berlin");
    Assert.assertEquals(coll.size(), 1);

  }
}
