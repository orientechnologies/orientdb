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
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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
    database.save(tomCruise);

    totalElements++;

    megRyan = database.newVertex("Actor");
    megRyan.setProperty("name", "Meg Ryan");
    database.save(megRyan);

    totalElements++;
    nicoleKidman = database.newVertex("Actor");
    nicoleKidman.setProperty("name", "Nicole Kidman");
    nicoleKidman.setProperty("attributeWithDotValue", "a.b");
    database.save(nicoleKidman);

    totalElements++;

    var topGun = database.newVertex("Movie");
    topGun.setProperty("name", "Top Gun");
    topGun.setProperty("year", 1986);
    database.save(topGun);

    totalElements++;
    var missionImpossible = database.newVertex("Movie");
    missionImpossible.setProperty("name", "Mission: Impossible");
    missionImpossible.setProperty("year", 1996);
    database.save(missionImpossible);

    totalElements++;
    var youHaveGotMail = database.newVertex("Movie");
    youHaveGotMail.setProperty("name", "You've Got Mail");
    youHaveGotMail.setProperty("year", 1998);
    database.save(youHaveGotMail);

    totalElements++;

    var e = database.newEdge(tomCruise, topGun, "actorIn");
    database.save(e);

    totalElements++;

    e = database.newEdge(megRyan, topGun, "actorIn");
    database.save(e);

    totalElements++;

    e = database.newEdge(tomCruise, missionImpossible, "actorIn");
    database.save(e);

    totalElements++;

    e = database.newEdge(megRyan, youHaveGotMail, "actorIn");
    database.save(e);

    totalElements++;

    e = database.newEdge(tomCruise, megRyan, "friend");
    database.save(e);

    totalElements++;
    e = database.newEdge(tomCruise, nicoleKidman, "married");
    e.setProperty("year", 1990);
    database.save(e);

    totalElements++;
  }

  public void traverseSQLAllFromActorNoWhere() {
    List<OResult> result1 =
        database.command("traverse * from " + tomCruise.getIdentity()).stream().toList();
    Assert.assertEquals(result1.size(), totalElements);
  }

  public void traverseAPIAllFromActorNoWhere() {
    List<OIdentifiable> result1 =
        new OTraverse().fields("*").target(tomCruise.getIdentity()).execute();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseSQLOutFromActor1Depth() {
    List<OResult> result1 =
        database
            .command("traverse out_ from " + tomCruise.getIdentity() + " while $depth <= 1")
            .stream()
            .toList();

    Assert.assertTrue(result1.size() != 0);
  }

  @Test
  public void traverseSQLMoviesOnly() {
    List<OResult> result1 =
        database.query("select from ( traverse * from Movie ) where @class = 'Movie'").stream()
            .toList();
    Assert.assertTrue(result1.size() > 0);
    for (OResult d : result1) {
      Assert.assertEquals(d.getElement().get().getSchemaType().get().getName(), "Movie");
    }
  }

  @Test
  public void traverseSQLPerClassFields() {
    List<OResult> result1 =
        database
            .command(
                "select from ( traverse out() from "
                    + tomCruise.getIdentity()
                    + ") where @class = 'Movie'")
            .stream()
            .toList();
    Assert.assertTrue(result1.size() > 0);
    for (OResult d : result1) {
      Assert.assertEquals(
          d.toElement().getSchemaType().map(x -> x.getName()).orElse(null), "Movie");
    }
  }

  @Test
  public void traverseSQLMoviesOnlyDepth() {
    List<OResult> result1 =
        database
            .command(
                "select from ( traverse * from "
                    + tomCruise.getIdentity()
                    + " while $depth <= 1 ) where @class = 'Movie'")
            .stream()
            .toList();
    Assert.assertTrue(result1.isEmpty());

    List<OResult> result2 =
        database
            .command(
                "select from ( traverse * from "
                    + tomCruise.getIdentity()
                    + " while $depth <= 2 ) where @class = 'Movie'")
            .stream()
            .toList();
    Assert.assertTrue(result2.size() > 0);
    for (OResult d : result2) {
      Assert.assertEquals(
          d.toElement().getSchemaType().map(x -> x.getName()).orElse(null), "Movie");
    }

    List<OResult> result3 =
        database
            .command(
                "select from ( traverse * from "
                    + tomCruise.getIdentity()
                    + " ) where @class = 'Movie'")
            .stream()
            .toList();
    Assert.assertTrue(result3.size() > 0);
    Assert.assertTrue(result3.size() > result2.size());
    for (OResult d : result3) {
      Assert.assertEquals(
          d.toElement().getSchemaType().map(x -> x.getName()).orElse(null), "Movie");
    }
  }

  @Test
  public void traverseSelect() {
    List<OResult> result1 =
        database.command("traverse * from ( select from Movie )").stream().toList();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseSQLSelectAndTraverseNested() {
    List<OResult> result1 =
        database
            .command(
                "traverse * from ( select from ( traverse * from "
                    + tomCruise.getIdentity()
                    + " while $depth <= 2 ) where @class = 'Movie' )")
            .stream()
            .toList();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNested() {
    List<OResult> result1 =
        database
            .command(
                "traverse * from ( select from ( traverse * from "
                    + tomCruise.getIdentity()
                    + " while $depth <= 2 ) where @class = 'Movie' )")
            .stream()
            .toList();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNestedDepthFirst() {
    List<OResult> result1 =
        database
            .command(
                "traverse * from ( select from ( traverse * from "
                    + tomCruise.getIdentity()
                    + " while $depth <= 2 strategy depth_first ) where @class = 'Movie' )")
            .stream()
            .toList();
    Assert.assertEquals(result1.size(), totalElements);
  }

  @Test
  public void traverseAPISelectAndTraverseNestedBreadthFirst() {
    List<OResult> result1 =
        database
            .command(
                "traverse * from ( select from ( traverse * from "
                    + tomCruise.getIdentity()
                    + " while $depth <= 2 strategy breadth_first ) where @class = 'Movie' )")
            .stream()
            .toList();
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
    List<OResult> result1 =
        database.command("traverse * from Movie while $depth < 2").stream().toList();
    List<OResult> result2 =
        database.command("select from ( traverse * from Movie while $depth < 2 )").stream()
            .toList();
    List<OResult> result3 =
        database
            .command("select from ( traverse * from Movie while $depth < 2 ) where true")
            .stream()
            .toList();
    List<OResult> result4 =
        database
            .command(
                "select from ( traverse * from Movie while $depth < 2 and ( true = true ) )"
                    + " where true")
            .stream()
            .toList();

    Assert.assertEquals(result1, result2);
    Assert.assertEquals(result1, result3);
    Assert.assertEquals(result1, result4);
  }

  @Test
  public void traverseNoConditionLimit1() {
    OResultSet result1 = database.command("traverse * from Movie limit 1");

    Assert.assertEquals(result1.stream().count(), 1);
  }

  @Test
  public void traverseAndFilterByAttributeThatContainsDotInValue() {
    // issue #4952
    List<OResult> result1 =
        database
            .command(
                "select from ( traverse out_married, in[attributeWithDotValue = 'a.b']  from "
                    + tomCruise.getIdentity()
                    + ")")
            .stream()
            .toList();
    Assert.assertTrue(result1.size() > 0);
    boolean found = false;
    for (OResult doc : result1) {
      String name = doc.getProperty("name");
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
    List<OResult> result1 =
        database
            .command(
                "select from (traverse out_married, in[attributeWithDotValue = :param1]  from "
                    + tomCruise.getIdentity()
                    + ")",
                params)
            .stream()
            .toList();
    Assert.assertTrue(result1.size() > 0);
    boolean found = false;
    for (OResult doc : result1) {
      String name = doc.getProperty("name");
      if ("Nicole Kidman".equals(name)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found);
  }

  @Test
  public void traverseAndCheckDepthInSelect() {
    List<OResult> result1 =
        database
            .command(
                "select *, $depth as d from ( traverse out_married  from "
                    + tomCruise.getIdentity()
                    + " while $depth < 2)")
            .stream()
            .toList();
    Assert.assertEquals(result1.size(), 2);
    boolean found = false;
    Integer i = 0;
    for (OResult doc : result1) {
      Integer depth = doc.getProperty("d");
      Assert.assertEquals(depth, i++);
    }
  }

  @Test
  public void traverseAndCheckReturn() {

    try {

      String q = "traverse in('married')  from " + nicoleKidman.getIdentity() + "";
      ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.copy();
      ODatabaseRecordThreadLocal.instance().set(db);
      List<OResult> result1 = db.command(q).stream().toList();
      Assert.assertEquals(result1.size(), 2);
      boolean found = false;
      Integer i = 0;
      for (OResult doc : result1) {
        Assert.assertTrue(doc.isVertex());
      }
    } finally {
      ODatabaseRecordThreadLocal.instance().set(database);
    }
  }
}
