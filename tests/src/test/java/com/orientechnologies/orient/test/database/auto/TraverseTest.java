/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandPredicate;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
@SuppressWarnings("unused")
public class TraverseTest extends DocumentDBBaseTest {

  private int totalElements = 0;
  private OVertex tomCruise;
  private OVertex megRyan;
  private OVertex nicoleKidman;

  @Parameters(value = "url")
  public TraverseTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void init() {
    database.createVertexClass("Movie");
    database.createVertexClass("Actor");

    database.createEdgeClass("actorIn");
    database.createEdgeClass("friend");
    database.createEdgeClass("married");

    tomCruise = database.newVertex("Actor");
    tomCruise.setProperty("name", "Tom Cruise");
    tomCruise.save();

    totalElements++;

    megRyan = database.newVertex("Actor");
    megRyan.setProperty("name", "Meg Ryan");
    megRyan.save();

    totalElements++;
    nicoleKidman = database.newVertex("Actor");
    nicoleKidman.setProperty("name", "Nicole Kidman");
    nicoleKidman.setProperty("attributeWithDotValue", "a.b");
    nicoleKidman.save();

    totalElements++;

    var topGun = database.newVertex("Movie");
    topGun.setProperty("name", "Top Gun");
    topGun.setProperty("year", 1986);
    topGun.save();

    totalElements++;
    var missionImpossible = database.newVertex("Movie");
    missionImpossible.setProperty("name", "Mission: Impossible");
    missionImpossible.setProperty("year", 1996);
    missionImpossible.save();

    totalElements++;
    var youHaveGotMail = database.newVertex("Movie");
    youHaveGotMail.setProperty("name", "You've Got Mail");
    youHaveGotMail.setProperty("year", 1998);
    youHaveGotMail.save();

    totalElements++;

    var e = database.newEdge(tomCruise, topGun, "actorIn");
    e.save();

    totalElements++;

    e = database.newEdge(megRyan, topGun, "actorIn");
    e.save();

    totalElements++;

    e = database.newEdge(tomCruise, missionImpossible, "actorIn");
    e.save();

    totalElements++;

    e = database.newEdge(megRyan, youHaveGotMail, "actorIn");
    e.save();

    totalElements++;

    e = database.newEdge(tomCruise, megRyan, "friend");
    e.save();

    totalElements++;
    e = database.newEdge(tomCruise, nicoleKidman, "married");
    e.setProperty("year", 1990);
    e.save();

    totalElements++;
  }

  public void traverseSQLAllFromActorNoWhere() {
    List<ODocument> result1 =
        database
            .command(new OSQLSynchQuery<ODocument>("traverse * from " + tomCruise.getIdentity()))
            .execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  public void traverseAPIAllFromActorNoWhere() {
    List<OIdentifiable> result1 =
        new OTraverse().fields("*").target(tomCruise.getIdentity()).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseSQLOutFromActor1Depth() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "traverse out_ from " + tomCruise.getIdentity() + " while $depth <= 1"))
            .execute();

    Assert.assertTrue(result1.size() != 0);
  }

  @Test
  public void traverseSQLMoviesOnly() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse any() from Movie ) where @class = 'Movie'"))
            .execute();
    Assert.assertTrue(result1.size() > 0);
    for (ODocument d : result1) {
      Assert.assertEquals(d.getClassName(), "Movie");
    }
  }

  @Test
  public void traverseSQLPerClassFields() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse out() from "
                        + tomCruise.getIdentity()
                        + ") where @class = 'Movie'"))
            .execute();
    Assert.assertTrue(result1.size() > 0);
    for (OElement d : result1) {
      Assert.assertEquals(d.getSchemaType().map(x -> x.getName()).orElse(null), "Movie");
    }
  }

  @Test
  public void traverseSQLMoviesOnlyDepth() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse * from "
                        + tomCruise.getIdentity()
                        + " while $depth <= 1 ) where @class = 'Movie'"))
            .execute();
    Assert.assertTrue(result1.isEmpty());

    List<ODocument> result2 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse * from "
                        + tomCruise.getIdentity()
                        + " while $depth <= 2 ) where @class = 'Movie'"))
            .execute();
    Assert.assertTrue(result2.size() > 0);
    for (ODocument d : result2) {
      Assert.assertEquals(d.getClassName(), "Movie");
    }

    List<ODocument> result3 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse * from "
                        + tomCruise.getIdentity()
                        + " ) where @class = 'Movie'"))
            .execute();
    Assert.assertTrue(result3.size() > 0);
    Assert.assertTrue(result3.size() > result2.size());
    for (ODocument d : result3) {
      Assert.assertEquals(d.getClassName(), "Movie");
    }
  }

  @Test
  public void traverseSelect() {
    List<ODocument> result1 =
        database
            .command(new OSQLSynchQuery<ODocument>("traverse * from ( select from Movie )"))
            .execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseSQLSelectAndTraverseNested() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "traverse * from ( select from ( traverse * from "
                        + tomCruise.getIdentity()
                        + " while $depth <= 2 ) where @class = 'Movie' )"))
            .execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNested() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "traverse * from ( select from ( traverse * from "
                        + tomCruise.getIdentity()
                        + " while $depth <= 2 ) where @class = 'Movie' )"))
            .execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNestedDepthFirst() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "traverse * from ( select from ( traverse * from "
                        + tomCruise.getIdentity()
                        + " while $depth <= 2 strategy depth_first ) where @class = 'Movie' )"))
            .execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNestedBreadthFirst() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "traverse * from ( select from ( traverse * from "
                        + tomCruise.getIdentity()
                        + " while $depth <= 2 strategy breadth_first ) where @class = 'Movie' )"))
            .execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseSQLIterating() {
    int cycles = 0;
    for (OIdentifiable id :
        new OSQLSynchQuery<ODocument>("traverse * from Movie while $depth < 2")) {
      cycles++;
    }
    Assert.assertTrue(cycles > 0);
  }

  @Test
  public void traverseAPIIterating() {
    int cycles = 0;
    for (OIdentifiable id :
        new OTraverse()
            .target(database.browseClass("Movie").iterator())
            .predicate(
                new OCommandPredicate() {
                  @Override
                  public Object evaluate(
                      OIdentifiable iRecord, ODocument iCurrentResult, OCommandContext iContext) {
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
    for (OIdentifiable id :
        new OTraverse()
            .target(database.browseClass("Movie").iterator())
            .predicate(new OSQLPredicate("$depth <= 2"))) {
      cycles++;
    }
    Assert.assertTrue(cycles > 0);
  }

  @Test
  public void traverseSelectIterable() {
    int cycles = 0;
    for (OIdentifiable id :
        new OSQLSynchQuery<ODocument>("select from ( traverse * from Movie while $depth < 2 )")) {
      cycles++;
    }
    Assert.assertTrue(cycles > 0);
  }

  @Test
  public void traverseSelectNoInfluence() {
    List<ODocument> result1 =
        database
            .command(new OSQLSynchQuery<ODocument>("traverse any() from Movie while $depth < 2"))
            .execute();
    List<ODocument> result2 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse any() from Movie while $depth < 2 )"))
            .execute();
    List<ODocument> result3 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse any() from Movie while $depth < 2 ) where true"))
            .execute();
    List<ODocument> result4 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse any() from Movie while $depth < 2 and ( true = true ) ) where true"))
            .execute();

    Assert.assertEquals(result1, result2);
    Assert.assertEquals(result1, result3);
    Assert.assertEquals(result1, result4);
  }

  @Test
  public void traverseNoConditionLimit1() {
    List<ODocument> result1 =
        database
            .command(new OSQLSynchQuery<ODocument>("traverse any() from Movie limit 1"))
            .execute();

    Assert.assertEquals(result1.size(), 1);
  }

  @Test
  public void traverseAndFilterByAttributeThatContainsDotInValue() {
    // issue #4952
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from ( traverse out_married, in[attributeWithDotValue = 'a.b']  from "
                        + tomCruise.getIdentity()
                        + ")"))
            .execute();
    Assert.assertTrue(result1.size() > 0);
    boolean found = false;
    for (ODocument doc : result1) {
      String name = doc.field("name");
      if ("Nicole Kidman".equals(name)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);
  }

  @Test
  public void traverseAndFilterWithNamedParam() {
    // issue #5225
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("param1", "a.b");
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select from (traverse out_married, in[attributeWithDotValue = :param1]  from "
                        + tomCruise.getIdentity()
                        + ")"))
            .execute(params);
    Assert.assertTrue(result1.size() > 0);
    boolean found = false;
    for (ODocument doc : result1) {
      String name = doc.field("name");
      if ("Nicole Kidman".equals(name)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);
  }

  @Test
  public void traverseAndCheckDepthInSelect() {
    List<ODocument> result1 =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select *, $depth as d from ( traverse out_married  from "
                        + tomCruise.getIdentity()
                        + " while $depth < 2)"))
            .execute();
    Assert.assertEquals(result1.size(), 2);
    boolean found = false;
    Integer i = 0;
    for (ODocument doc : result1) {
      Integer depth = doc.field("d");
      Assert.assertEquals(depth, i++);
    }
  }

  @Test
  public void traverseAndCheckReturn() {

    try {

      String q = "traverse in('married')  from " + nicoleKidman.getIdentity() + "";
      ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.copy();
      ODatabaseRecordThreadLocal.instance().set(db);
      List<Object> result1 = db.command(new OSQLSynchQuery<ODocument>(q)).execute();
      Assert.assertEquals(result1.size(), 2);
      boolean found = false;
      Integer i = 0;
      for (Object doc : result1) {
        Assert.assertTrue(((OElement) doc).isVertex());
      }
    } finally {
      ODatabaseRecordThreadLocal.instance().set(database);
    }
  }
}
