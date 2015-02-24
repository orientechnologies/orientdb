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

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class PreparedStatementTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public PreparedStatementTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    database.command(new OCommandSQL("CREATE CLASS PreparedStatementTest1")).execute();
    database.command(new OCommandSQL("insert into PreparedStatementTest1 (name, surname) values ('foo1', 'bar1')")).execute();
    database.command(new OCommandSQL("insert into PreparedStatementTest1 (name, listElem) values ('foo2', ['bar2'])")).execute();

  }

  @Test
  public void testUnnamedParamFlat() {
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from PreparedStatementTest1 where name = ?")).execute("foo1");

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);

  }

  @Test
  public void testNamedParamFlat() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from PreparedStatementTest1 where name = :name")).execute(params);

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);

  }

  @Test
  public void testUnnamedParamInArray() {
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from PreparedStatementTest1 where name in [?]")).execute("foo1");

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);

  }

  @Test
  public void testNamedParamInArray() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from PreparedStatementTest1 where name in [:name]")).execute(params);

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);

  }

  @Test
  public void testUnnamedParamInArray2() {
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from PreparedStatementTest1 where name in [?, 'antani']")).execute("foo1");

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);

  }

  @Test
  public void testNamedParamInArray2() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from PreparedStatementTest1 where name in [:name, 'antani']")).execute(params);

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }



  @Test
  public void testSubqueryUnnamedParamFlat() {
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from (select from PreparedStatementTest1 where name = ?) where name = ?")).execute("foo1", "foo1");

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);

  }

  @Test
  public void testSubqueryNamedParamFlat() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select from (select from PreparedStatementTest1 where name = :name) where name = :name")).execute(params);

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);

  }

  @Test
  public void testFunction() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("one", 1);
    params.put("three", 3);
    Iterable<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select max(:one, :three) as maximo")).execute(params);

    boolean found = false;
    for (ODocument doc : result) {
      found = true;
      Assert.assertEquals(doc.field("maximo"), 3);
    }
    Assert.assertTrue(found);

  }

}
