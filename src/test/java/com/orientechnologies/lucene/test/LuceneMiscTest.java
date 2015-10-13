/*
 *
 *  * Copyright 2015 Orient Technologies.
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

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 12/10/15.
 */

@Test(groups = "embedded")
public class LuceneMiscTest {

  @Test
  public void dottedNotationTest() {

    OrientGraphNoTx db = new OrientGraphNoTx("memory:dotted");

    try {
      OSchema schema = db.getRawGraph().getMetadata().getSchema();
      OClass v = schema.getClass("V");
      OClass e = schema.getClass("E");
      OClass author = schema.createClass("Author");
      author.setSuperClass(v);
      author.createProperty("name", OType.STRING);

      OClass song = schema.createClass("Song");
      song.setSuperClass(v);
      song.createProperty("title", OType.STRING);

      OClass authorOf = schema.createClass("AuthorOf");
      authorOf.createProperty("in", OType.LINK, song);
      authorOf.setSuperClass(e);

      db.command(new OCommandSQL("create index AuthorOf.in on AuthorOf (in) NOTUNIQUE")).execute();
      db.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();

      OrientVertex authorVertex = db.addVertex("class:Author", new String[] { "name", "Bob Dylan" });
      OrientVertex songVertex = db.addVertex("class:Song", new String[] { "title", "hurricane" });

      authorVertex.addEdge("AuthorOf", songVertex);

      List<Object> results = db.getRawGraph().command(new OCommandSQL("select from AuthorOf")).execute();
      Assert.assertEquals(results.size(), 1);

      results = db.getRawGraph().command(new OCommandSQL("select from AuthorOf where in.title lucene 'hurricane'")).execute();

      Assert.assertEquals(results.size(), 1);
    } finally {
      db.drop();
    }
  }

  @Test(expectedExceptions = OCommandExecutionException.class)
  public void executionExceptionTest() {

    OrientGraphNoTx db = new OrientGraphNoTx("memory:dotted");

    try {
      OSchema schema = db.getRawGraph().getMetadata().getSchema();
      OClass v = schema.getClass("V");
      OClass e = schema.getClass("E");
      OClass author = schema.createClass("Author");
      author.setSuperClass(v);
      author.createProperty("name", OType.STRING);

      OClass song = schema.createClass("Song");
      song.setSuperClass(v);
      song.createProperty("title", OType.STRING);

      OClass authorOf = schema.createClass("AuthorOf");
      authorOf.createProperty("in", OType.LINK, song);
      authorOf.setSuperClass(e);

      db.command(new OCommandSQL("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE")).execute();

      OrientVertex authorVertex = db.addVertex("class:Author", new String[] { "name", "Bob Dylan" });
      OrientVertex songVertex = db.addVertex("class:Song", new String[] { "title", "hurricane" });

      authorVertex.addEdge("AuthorOf", songVertex);

      db.getRawGraph().command(new OCommandSQL("select from Song where name lucene 'hurricane'")).execute();

    } finally {
      db.drop();
    }
  }
}
