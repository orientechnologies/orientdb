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

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by enricorisa on 28/06/14. */
public class OLuceneListIndexingTest extends OLuceneBaseTest {

  @Before
  public void init() {

    OSchema schema = db.getMetadata().getSchema();

    OClass person = schema.createClass("Person");
    person.createProperty("name", OType.STRING);
    person.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    //noinspection EmptyTryBlock
    try (OResultSet command =
        db.command("create index Person.name_tags on Person (name,tags) FULLTEXT ENGINE LUCENE")) {}

    OClass city = schema.createClass("City");
    city.createProperty("name", OType.STRING);
    city.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    //noinspection EmptyTryBlock
    try (OResultSet command =
        db.command("create index City.tags on City (tags) FULLTEXT ENGINE LUCENE")) {}
  }

  @Test
  public void testIndexingList() {

    OSchema schema = db.getMetadata().getSchema();

    // Rome
    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    doc.field("tags", Arrays.asList("Beautiful", "Touristic", "Sunny"));

    db.save(doc);

    OIndex tagsIndex = schema.getClass("City").getClassIndex("City.tags");
    Collection<?> coll;
    try (Stream<ORID> stream = tagsIndex.getInternal().getRids("Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    doc = db.load((ORID) coll.iterator().next());

    assertThat(doc.<String>field("name")).isEqualTo("Rome");

    // London
    doc = new ODocument("City");
    doc.field("name", "London");
    doc.field("tags", Arrays.asList("Beautiful", "Touristic", "Sunny"));
    db.save(doc);

    try (Stream<ORID> stream = tagsIndex.getInternal().getRids("Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    // modify london: it is rainy
    List<String> tags = doc.field("tags");
    tags.remove("Sunny");
    tags.add("Rainy");

    db.save(doc);

    try (Stream<ORID> stream = tagsIndex.getInternal().getRids("Rainy")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (Stream<ORID> stream = tagsIndex.getInternal().getRids("Beautiful")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    try (Stream<ORID> stream = tagsIndex.getInternal().getRids("Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (OResultSet query = db.query("select from City where search_class('Beautiful') =true ")) {

      assertThat(query).hasSize(2);
    }
  }

  @Test
  @Ignore
  public void testCompositeIndexList() {

    OSchema schema = db.getMetadata().getSchema();

    ODocument doc = new ODocument("Person");
    doc.field("name", "Enrico");
    doc.field("tags", Arrays.asList("Funny", "Tall", "Geek"));

    db.save(doc);
    OIndex idx = schema.getClass("Person").getClassIndex("Person.name_tags");
    Collection<?> coll;
    try (Stream<ORID> stream = idx.getInternal().getRids("Enrico")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(coll).hasSize(3);

    doc = new ODocument("Person");
    doc.field("name", "Jared");
    doc.field("tags", Arrays.asList("Funny", "Tall"));

    db.save(doc);

    try (Stream<ORID> stream = idx.getInternal().getRids("Jared")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(coll).hasSize(2);

    List<String> tags = doc.field("tags");

    tags.remove("Funny");
    tags.add("Geek");

    db.save(doc);

    try (Stream<ORID> stream = idx.getInternal().getRids("Funny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (Stream<ORID> stream = idx.getInternal().getRids("Geek")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    try (OResultSet query =
        db.query("select from Person where search_class('name:Enrico') =true ")) {
      assertThat(query).hasSize(1);
      try (OResultSet queryTwo =
          db.query("select from (select from Person search_class('name:Enrico')=true)")) {

        assertThat(queryTwo).hasSize(1);
        try (OResultSet queryThree =
            db.query("select from Person where search_class('Jared')=true")) {

          assertThat(queryThree).hasSize(1);
          try (OResultSet queryFour =
              db.query("select from Person where search_class('Funny') =true")) {

            assertThat(queryFour).hasSize(1);
            try (OResultSet queryFive =
                db.query("select from Person where search_class('Geek')=true")) {

              assertThat(queryFive).hasSize(2);
              try (OResultSet querySix =
                  db.query(
                      "select from Person where search_class('(name:Enrico AND tags:Geek) ')=true")) {
                assertThat(querySix).hasSize(1);
              }
            }
          }
        }
      }
    }
  }
}
