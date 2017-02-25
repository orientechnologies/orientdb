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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 18/09/15.
 */
public class LuceneMiscTest extends BaseLuceneTest {

  @Test
  public void testDoubleLucene() {

    db.command("create class Test extends V");
    db.command("create property Test.attr1 string");
    db.command("create index Test.attr1 on Test(attr1) FULLTEXT ENGINE LUCENE");
    db.command("create property Test.attr2 string");
    db.command("create index Test.attr2 on Test(attr2) FULLTEXT ENGINE LUCENE");

    db.command("insert into Test set attr1='foo', attr2='bar'");
    db.command("insert into Test set attr1='bar', attr2='foo'");

    OResultSet results = db
        .query("select from Test where search_index('Test.attr1',\"foo*\") =true OR search_index('Test.attr2', \"foo*\")=true  ");
    assertThat(results).hasSize(2);
    results.close();

    results = db.query("select from Test where SEARCH_FIELDS( 'attr1', 'bar') = true OR SEARCH_FIELDS('attr2', 'bar*' )= true ");
    assertThat(results).hasSize(2);
    results.close();

    results = db.query("select from Test where SEARCH_FIELDS( 'attr1', 'foo*') = true AND SEARCH_FIELDS('attr2', 'bar*') = true");
    assertThat(results).hasSize(1);
    results.close();

    results = db.query("select from Test where SEARCH_FIELDS( 'attr1', 'bar*') = true AND SEARCH_FIELDS('attr2', 'foo*')= true");
    assertThat(results).hasSize(1);
    results.close();

  }

  @Test
  public void testSubLucene() {

    db.command("create class Person extends V");

    db.command("create property Person.name string");

    db.command("create index Person.name on Person(name) FULLTEXT ENGINE LUCENE");

    db.command("insert into Person set name='Enrico', age=18");

    String query = "select  from (select from Person where age = 18) where search_fields('name','Enrico') = true";
    OResultSet results = db.query(query);

    assertThat(results).hasSize(1);
    results.close();

    // WITH PROJECTION it works using index directly

    query = "select  from (select name from Person where age = 18) where search_index('Person.name','Enrico') = true";
    results = db.query(query);
    assertThat(results).hasSize(1);

  }

  @Test
  public void testNamedParams() {

    db.command(new OCommandSQL("create class Test extends V")).execute();

    db.command(new OCommandSQL("create property Test.attr1 string")).execute();

    db.command(new OCommandSQL("create index Test.attr1 on Test (attr1) fulltext engine lucene")).execute();

    db.command(new OCommandSQL("insert into Test set attr1='foo', attr2='bar'")).execute();

    OSQLSynchQuery query = new OSQLSynchQuery("select from Test where attr1 lucene :name");
    Map params = new HashMap();
    params.put("name", "FOO or");
    List results = db.command(query).execute(params);
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void dottedNotationTest() {

    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    OClass e = schema.getClass("E");
    OClass author = schema.createClass("Author", v);
    author.createProperty("name", OType.STRING);

    OClass song = schema.createClass("Song", v);
    song.createProperty("title", OType.STRING);

    OClass authorOf = schema.createClass("AuthorOf", e);
    authorOf.createProperty("in", OType.LINK, song);
    db.commit();

    db.command(new OCommandSQL("create index AuthorOf.in on AuthorOf (in) NOTUNIQUE")).execute();
    db.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();

    OVertex authorVertex = db.newVertex("Author");
    authorVertex.setProperty("name", "Bob Dylan");
    db.save(authorVertex);

    OVertex songVertex = db.newVertex("Song");
    songVertex.setProperty("title", "hurricane");
    db.save(songVertex);
    OEdge edge = authorVertex.addEdge(songVertex, "AuthorOf");
    db.save(edge);

    List<Object> results = db.command(new OCommandSQL("select from AuthorOf")).execute();
    Assert.assertEquals(results.size(), 1);

    results = db.command(new OCommandSQL("select from AuthorOf where in.title lucene 'hurricane'")).execute();

    Assert.assertEquals(results.size(), 1);
  }

}
