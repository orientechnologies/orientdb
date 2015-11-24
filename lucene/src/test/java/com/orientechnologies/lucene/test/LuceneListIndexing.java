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

import com.orientechnologies.orient.core.id.ORID;
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

import java.util.*;

/**
 * Created by enricorisa on 28/06/14.
 */

@Test(groups = "embedded")
public class LuceneListIndexing extends BaseLuceneTest {

  public LuceneListIndexing() {
    super();
  }

  public LuceneListIndexing(boolean remote) {
    super();
  }

  @Override
  protected String getDatabaseName() {
    return "listIndexing";
  }

  @BeforeClass
  public void init() {
    initDB();

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    OClass person = schema.createClass("Person");

    person.createProperty("name", OType.STRING);
    person.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);

    oClass.createProperty("name", OType.STRING);
    oClass.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("create index City.tags on City (tags) FULLTEXT ENGINE LUCENE")).execute();

    databaseDocumentTx.command(new OCommandSQL("create index Person.name_tags on Person (name,tags) FULLTEXT ENGINE LUCENE"))
        .execute();
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  @Test
  public void testIndexingList() throws Exception {

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Beautiful");
        add("Touristic");
        add("Sunny");
      }
    });
    databaseDocumentTx.save(doc);
    OIndex idx = schema.getClass("City").getClassIndex("City.tags");
    Collection<?> coll = (Collection<?>) idx.get("Sunny");
    Assert.assertEquals(coll.size(), 1);

    doc = databaseDocumentTx.load((ORID) coll.iterator().next());
    Assert.assertEquals(doc.field("name"), "Rome");

    doc = new ODocument("City");
    doc.field("name", "London");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Beautiful");
        add("Touristic");
        add("Sunny");
      }
    });
    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Sunny");
    Assert.assertEquals(coll.size(), 2);

    List<String> tags = doc.field("tags");

    tags.remove("Sunny");
    tags.add("Rainy");

    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Sunny");
    Assert.assertEquals(coll.size(), 1);

    coll = (Collection<?>) idx.get("Rainy");
    Assert.assertEquals(coll.size(), 1);

    coll = (Collection<?>) idx.get("Beautiful");
    Assert.assertEquals(coll.size(), 2);

  }

  @Test
  public void testCompositeIndexList() {

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();

    ODocument doc = new ODocument("Person");
    doc.field("name", "Enrico");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Funny");
        add("Tall");
        add("Geek");
      }
    });
    Set<?> set;
    databaseDocumentTx.save(doc);
    OIndex idx = schema.getClass("Person").getClassIndex("Person.name_tags");
    Collection<?> coll = (Collection<?>) idx.get("Enrico");
    set = new HashSet(coll);
    Assert.assertEquals(set.size(), 1);

    doc = new ODocument("Person");
    doc.field("name", "Jared");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Funny");
        add("Tall");
      }
    });
    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Jared");

    set = new HashSet(coll);
    Assert.assertEquals(set.size(), 1);

    List<String> tags = doc.field("tags");

    tags.remove("Funny");
    tags.add("Geek");

    databaseDocumentTx.save(doc);

    coll = (Collection<?>) idx.get("Funny");
    set = new HashSet(coll);
    Assert.assertEquals(set.size(), 1);

    coll = (Collection<?>) idx.get("Geek");
    set = new HashSet(coll);
    Assert.assertEquals(set.size(), 2);

    List<?> query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Enrico'"));

    Assert.assertEquals(query.size(), 1);

    query = databaseDocumentTx.query(new OSQLSynchQuery<Object>(
        "select from (select from Person where [name,tags] lucene 'Enrico')"));

    Assert.assertEquals(query.size(), 1);

    query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Jared'"));

    Assert.assertEquals(query.size(), 1);

    query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Funny'"));

    Assert.assertEquals(query.size(), 1);

    query = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Geek'"));

    Assert.assertEquals(query.size(), 2);

    query = databaseDocumentTx.query(new OSQLSynchQuery<Object>(
        "select from Person where [name,tags] lucene '(name:Enrico AND tags:Geek)'"));

    Assert.assertEquals(query.size(), 1);
  }
}
