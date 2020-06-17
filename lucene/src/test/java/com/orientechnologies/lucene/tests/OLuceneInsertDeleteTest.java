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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 28/06/14. */
public class OLuceneInsertDeleteTest extends OLuceneBaseTest {

  @Before
  public void init() {

    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    oClass.createProperty("name", OType.STRING);
    //noinspection EmptyTryBlock
    try (OResultSet resultSet =
        db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE")) {}
  }

  @Test
  public void testInsertUpdateWithIndex() {

    db.getMetadata().reload();
    OSchema schema = db.getMetadata().getSchema();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");
    db.save(doc);

    OIndex idx = schema.getClass("City").getClassIndex("City.name");
    Collection<?> coll;
    try (Stream<ORID> stream = idx.getInternal().getRids("Rome")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(coll).hasSize(1);
    assertThat(idx.getInternal().size()).isEqualTo(1);

    OIdentifiable next = (OIdentifiable) coll.iterator().next();
    doc = db.load(next.<ORecord>getRecord());

    db.delete(doc);

    try (Stream<ORID> stream = idx.getInternal().getRids("Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(0);
    assertThat(idx.getInternal().size()).isEqualTo(0);
  }

  @Test
  public void testDeleteWithQueryOnClosedIndex() throws Exception {

    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql")) {
      //noinspection EmptyTryBlock
      try (OResultSet resultSet = db.execute("sql", getScriptFromStream(stream))) {}
    }

    //noinspection EmptyTryBlock
    try (OResultSet resultSet =
        db.command(
            "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata {'closeAfterInterval':1000 , 'firstFlushAfter':1000 }")) {}

    try (OResultSet docs = db.query("select from Song where title lucene 'mountain'")) {

      assertThat(docs).hasSize(4);
      TimeUnit.SECONDS.sleep(5);
      docs.close();

      //noinspection EmptyTryBlock
      try (OResultSet command =
          db.command("delete vertex from Song where title lucene 'mountain'")) {}

      try (OResultSet resultSet = db.query("select from Song where  title lucene 'mountain'")) {
        assertThat(resultSet).hasSize(0);
      }
    }
  }
}
