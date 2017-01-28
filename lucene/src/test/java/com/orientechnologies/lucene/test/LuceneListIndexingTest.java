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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by enricorisa on 28/06/14.
 */

public class LuceneListIndexingTest extends BaseLuceneTest {

  public LuceneListIndexingTest() {
    super();
  }

  @Before
  public void init() {
    //    System.¢setProperty("orientdb.test.env","ci");

    OSchema schema = db.getMetadata().getSchema();

    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);
    person.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    db.command(new OCommandSQL("create index Person.name_tags on Person (name,tags) FULLTEXT ENGINE LUCENE")).execute();

    OClass city = schema.createClass("City");
    city.createProperty("name", OType.STRING);
    city.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    db.command(new OCommandSQL("create index City.tags on City (tags) FULLTEXT ENGINE LUCENE")).execute();

  }

  @Test
  public void testIndexingList() throws Exception {

    OSchema schema = db.getMetadata().getSchema();

    //Rome
    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Beautiful");
        add("Touristic");
        add("Sunny");
      }
    });

    db.save(doc);

    OIndex tagsIndex = schema.getClass("City").getClassIndex("City.tags");
    Collection<?> coll = (Collection<?>) tagsIndex.get("Sunny");
    assertThat(coll).hasSize(1);

    doc = db.load((ORID) coll.iterator().next());

    assertThat(doc.<String>field("name")).isEqualTo("Rome");

    //London
    doc = new ODocument("City");
    doc.field("name", "London");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Beautiful");
        add("Touristic");
        add("Sunny");
      }
    });
    db.save(doc);

    coll = (Collection<?>) tagsIndex.get("Sunny");
    assertThat(coll).hasSize(2);

    //modify london: it is rainy
    List<String> tags = doc.field("tags");
    tags.remove("Sunny");
    tags.add("Rainy");

    db.save(doc);

    coll = (Collection<?>) tagsIndex.get("Rainy");
    assertThat(coll).hasSize(1);

    coll = (Collection<?>) tagsIndex.get("Beautiful");
    assertThat(coll).hasSize(2);

    coll = (Collection<?>) tagsIndex.get("Sunny");
    assertThat(coll).hasSize(1);

  }

  @Test
  public void testCompositeIndexList() {

    OSchema schema = db.getMetadata().getSchema();

    ODocument doc = new ODocument("Person");
    doc.field("name", "Enrico");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Funny");
        add("Tall");
        add("Geek");
      }
    });

    db.save(doc);
    OIndex idx = schema.getClass("Person").getClassIndex("Person.name_tags");
    Collection<?> coll = (Collection<?>) idx.get("Enrico");

    assertThat(coll).hasSize(3);

    doc = new ODocument("Person");
    doc.field("name", "Jared");
    doc.field("tags", new ArrayList<String>() {
      {
        add("Funny");
        add("Tall");
      }
    });
    db.save(doc);

    coll = (Collection<?>) idx.get("Jared");

    assertThat(coll).hasSize(2);

    List<String> tags = doc.field("tags");

    tags.remove("Funny");
    tags.add("Geek");

    db.save(doc);

    coll = (Collection<?>) idx.get("Funny");
    assertThat(coll).hasSize(1);

    coll = (Collection<?>) idx.get("Geek");
    assertThat(coll).hasSize(2);

    List<?> query = db.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Enrico'"));

    assertThat(query).hasSize(1);

    query = db.query(new OSQLSynchQuery<Object>("select from (select from Person where [name,tags] lucene 'Enrico')"));

    assertThat(query).hasSize(1);

    query = db.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Jared'"));

    assertThat(query).hasSize(1);

    query = db.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Funny'"));

    assertThat(query).hasSize(1);

    query = db.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene 'Geek'"));

    assertThat(query).hasSize(2);

    query = db.query(new OSQLSynchQuery<Object>("select from Person where [name,tags] lucene '(name:Enrico AND tags:Geek)'"));

    assertThat(query).hasSize(1);
  }

  @Test
  public void rname() throws Exception {

    final OClass c1 = db.createVertexClass("C1");
    c1.createProperty("p1", OType.STRING);

    final ODocument metadata = new ODocument();
    metadata.field("default", "org.apache.lucene.analysis.en.EnglishAnalyzer");

    c1.createIndex("p1", "FULLTEXT", null, metadata, "LUCENE", new String[] { "p1" });

    final OVertex vertex = db.newVertex("C1");
    vertex.setProperty("p1", "testing");
    db.save(vertex);
    db.commit();

    final List<ODocument> search = db.query(new OSQLSynchQuery<ODocument>("SELECT from C1 WHERE p1 LUCENE \"tested\""));

    assertThat(search).hasSize(1);

  }
}
