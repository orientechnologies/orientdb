/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
@SuppressWarnings("unused")
public class TraverseTest {
  private int            totalElements = 0;
  private OGraphDatabase database;
  private ODocument      tomCruise;
  private ODocument      megRyan;
  private ODocument      nicoleKidman;

  @Parameters(value = "url")
  public TraverseTest(@Optional(value = "memory:test") String iURL) {
    database = new OGraphDatabase(iURL);
  }

  @BeforeClass
  public void init() {
    if ("memory:test".equals(database.getURL()))
      database.create();
    else
      database.open("admin", "admin");

    database.createVertexType("Movie");
    database.createVertexType("Actor");

    tomCruise = database.createVertex("Actor").field("name", "Tom Cruise");
    totalElements++;
    megRyan = database.createVertex("Actor").field("name", "Meg Ryan");
    totalElements++;
    nicoleKidman = database.createVertex("Actor").field("name", "Nicol Kidman");
    totalElements++;

    ODocument topGun = database.createVertex("Movie").field("name", "Top Gun").field("year", 1986);
    totalElements++;
    ODocument missionImpossible = database.createVertex("Movie").field("name", "Mission: Impossible").field("year", 1996);
    totalElements++;
    ODocument youHaveGotMail = database.createVertex("Movie").field("name", "You've Got Mail").field("year", 1998);
    totalElements++;

    database.createEdge(tomCruise, topGun).field("actorIn");
    totalElements++;
    database.createEdge(megRyan, topGun).field("actorIn");
    totalElements++;
    database.createEdge(tomCruise, missionImpossible).field("actorIn");
    totalElements++;
    database.createEdge(megRyan, youHaveGotMail).field("actorIn");
    totalElements++;

    database.createEdge(tomCruise, megRyan).field("friend", true);
    totalElements++;
    database.createEdge(tomCruise, nicoleKidman).field("married", true).field("year", 1990);
    totalElements++;

    tomCruise.save();
  }

  @AfterClass
  public void deinit() {
    database.close();
  }

  public void traverseSQLAllFromActorNoWhere() {
    List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse * from " + tomCruise.getIdentity()))
        .execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  public void traverseAPIAllFromActorNoWhere() {
    List<OIdentifiable> result1 = new OTraverse().fields("*").target(tomCruise.getIdentity()).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  public void traverseSQLOutFromOneActorNoWhere() {
    database.command(new OSQLSynchQuery<ODocument>("traverse out_ from " + tomCruise.getIdentity())).execute();
  }

  public void traverseAPIOutFromOneActorNoWhere() {
    new OTraverse().field("out_").target(tomCruise.getIdentity()).execute();
  }

  @Test
  public void traverseSQLOutFromActor1Depth() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("traverse out_ from " + tomCruise.getIdentity() + " while $depth <= 1")).execute();

    Assert.assertTrue(result1.size() != 0);

    for (ODocument d : result1) {
    }
  }

  @Test
  public void traverseSQLDept02() {
    List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse any() from Movie while $depth < 2"))
        .execute();
  }

  @Test
  public void traverseSQLOldSyntax() {
    List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse any() from Movie where $depth < 2"))
        .execute();
  }

  @Test
  public void traverseSQLMoviesOnly() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie ) where @class = 'Movie'")).execute();
    Assert.assertTrue(result1.size() > 0);
    for (ODocument d : result1) {
      Assert.assertEquals(d.getClassName(), "Movie");
    }
  }

  @Test
  public void traverseSQLPerClassFields() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse V.out_, E.in from " + tomCruise.getIdentity()
            + ") where @class = 'Movie'")).execute();
    Assert.assertTrue(result1.size() > 0);
    for (ODocument d : result1) {
      Assert.assertEquals(d.getClassName(), "Movie");
    }
  }

  @Test
  public void traverseSQLMoviesOnlyDepth() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse * from " + tomCruise.getIdentity()
            + " while $depth <= 1 ) where @class = 'Movie'")).execute();
    Assert.assertTrue(result1.isEmpty());

    List<ODocument> result2 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse * from " + tomCruise.getIdentity()
            + " while $depth <= 2 ) where @class = 'Movie'")).execute();
    Assert.assertTrue(result2.size() > 0);
    for (ODocument d : result2) {
      Assert.assertEquals(d.getClassName(), "Movie");
    }

    List<ODocument> result3 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse * from " + tomCruise.getIdentity() + " ) where @class = 'Movie'"))
        .execute();
    Assert.assertTrue(result3.size() > 0);
    Assert.assertTrue(result3.size() > result2.size());
    for (ODocument d : result3) {
      Assert.assertEquals(d.getClassName(), "Movie");
    }
  }

  @Test
  public void traverseSelect() {
    List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse * from ( select from Movie )")).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseSQLSelectAndTraverseNested() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("traverse * from ( select from ( traverse * from " + tomCruise.getIdentity()
            + " while $depth <= 2 ) where @class = 'Movie' )")).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNested() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("traverse * from ( select from ( traverse * from " + tomCruise.getIdentity()
            + " while $depth <= 2 ) where @class = 'Movie' )")).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNestedDepthFirst() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("traverse * from ( select from ( traverse * from " + tomCruise.getIdentity()
            + " while $depth <= 2 strategy depth_first ) where @class = 'Movie' )")).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNestedBreadthFirst() {
    List<ODocument> result1 = database.command(
        new OSQLSynchQuery<ODocument>("traverse * from ( select from ( traverse * from " + tomCruise.getIdentity()
            + " while $depth <= 2 strategy breadth_first ) where @class = 'Movie' )")).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseSQLIterating() {
    int cycles = 0;
    for (OIdentifiable id : new OSQLSynchQuery<ODocument>("traverse * from Movie while $depth < 2")) {
      cycles++;
    }
    Assert.assertTrue(cycles > 0);
  }

  @Test
  public void traverseAPIIterating() {
    int cycles = 0;
    for (OIdentifiable id : new OTraverse().target(database.browseClass("Movie").iterator()).predicate(new OCommandPredicate() {
      public Object evaluate(ORecord<?> iRecord, ODocument iCurrentResult, OCommandContext iContext) {
        return ((Integer) iContext.getVariable("depth")) <= 2;
      }
    })) {
      cycles++;
    }
    Assert.assertTrue(cycles > 0);
  }

  @Test
  public void traverseAPIandSQLIterating() {
    int cycles = 0;
    for (OIdentifiable id : new OTraverse().target(database.browseClass("Movie").iterator()).predicate(
        new OSQLPredicate("$depth <= 2"))) {
      cycles++;
    }
    Assert.assertTrue(cycles > 0);
  }

  @Test
  public void traverseSelectIterable() {
    int cycles = 0;
    for (OIdentifiable id : new OSQLSynchQuery<ODocument>("select from ( traverse * from Movie while $depth < 2 )")) {
      cycles++;
    }
    Assert.assertTrue(cycles > 0);
  }

  @Test
  public void traverseSelectNoInfluence() {
    List<ODocument> result1 = database.command(new OSQLSynchQuery<ODocument>("traverse any() from Movie while $depth < 2"))
        .execute();
    List<ODocument> result2 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie while $depth < 2 )")).execute();
    List<ODocument> result3 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie while $depth < 2 ) where true")).execute();
    List<ODocument> result4 = database.command(
        new OSQLSynchQuery<ODocument>("select from ( traverse any() from Movie while $depth < 2 and ( true = true ) ) where true"))
        .execute();

    Assert.assertEquals(result1, result2);
    Assert.assertEquals(result1, result3);
    Assert.assertEquals(result1, result4);
  }

}
