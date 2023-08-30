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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 18/09/15. */
public class LuceneMiscTest extends BaseLuceneTest {

  // TODO Re-enable when removed check syntax on ODB
  public void testDoubleLucene() {

    db.command("create class Test extends V").close();
    db.command("create property Test.attr1 string").close();
    db.command("create index Test.attr1 on Test (attr1) fulltext engine lucene").close();
    db.command("create property Test.attr2 string").close();
    db.command("create index Test.attr2 on Test (attr2) fulltext engine lucene").close();
    db.command("insert into Test set attr1='foo', attr2='bar'").close();
    db.command("insert into Test set attr1='bar', attr2='foo'").close();

    OResultSet results =
        db.command("select from Test where attr1 lucene 'foo*' OR attr2 lucene 'foo*'");
    Assert.assertEquals(2, results.stream().count());

    results = db.command("select from Test where attr1 lucene 'bar*' OR attr2 lucene 'bar*'");

    Assert.assertEquals(2, results.stream().count());

    results = db.command("select from Test where attr1 lucene 'foo*' AND attr2 lucene 'bar*'");

    Assert.assertEquals(1, results.stream().count());

    results = db.command("select from Test where attr1 lucene 'bar*' AND attr2 lucene 'foo*'");

    Assert.assertEquals(1, results.stream().count());
  }

  @Test
  public void testSubLucene() {

    db.command("create class Person extends V").close();

    db.command("create property Person.name string").close();

    db.command("create index Person.name on Person (name) fulltext engine lucene").close();

    db.command("insert into Person set name='Enrico', age=18").close();

    OResultSet results =
        db.query("select  from (select from Person where age = 18) where name lucene 'Enrico'");
    Assert.assertEquals(1, results.stream().count());

    // WITH PROJECTION does not work as the class is missing

    results =
        db.query(
            "select  from (select name  from Person where age = 18) where name lucene 'Enrico'");
    Assert.assertEquals(0, results.stream().count());
  }

  @Test
  public void testNamedParams() {

    db.command("create class Test extends V").close();
    ;

    db.command("create property Test.attr1 string").close();

    db.command("create index Test.attr1 on Test (attr1) fulltext engine lucene").close();

    db.command("insert into Test set attr1='foo', attr2='bar'").close();

    Map params = new HashMap();
    params.put("name", "FOO or");
    OResultSet results = db.query("select from Test where attr1 lucene :name", params);
    Assert.assertEquals(1, results.stream().count());
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

    db.command("create index AuthorOf.in on AuthorOf (in) NOTUNIQUE").close();
    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE").close();

    OVertex authorVertex = db.newVertex("Author");
    authorVertex.setProperty("name", "Bob Dylan");
    db.save(authorVertex);

    OVertex songVertex = db.newVertex("Song");
    songVertex.setProperty("title", "hurricane");
    db.save(songVertex);
    OEdge edge = authorVertex.addEdge(songVertex, "AuthorOf");
    db.save(edge);

    OResultSet results = db.query("select from AuthorOf");
    Assert.assertEquals(results.stream().count(), 1);

    List<?> results1 =
        db.command(new OCommandSQL("select from AuthorOf where in.title lucene 'hurricane'"))
            .execute();

    Assert.assertEquals(results1.size(), 1);
  }

  @Test
  public void testUnderscoreField() {

    db.command("create class Test extends V").close();

    db.command("create property V._attr1 string").close();

    db.command("create index V._attr1 on V (_attr1) fulltext engine lucene").close();

    db.command("insert into Test set _attr1='anyPerson', attr2='bar'").close();

    Map params = new HashMap();
    params.put("name", "anyPerson");
    OResultSet results = db.command("select from Test where _attr1 lucene :name", params);
    Assert.assertEquals(results.stream().count(), 1);
  }
}
