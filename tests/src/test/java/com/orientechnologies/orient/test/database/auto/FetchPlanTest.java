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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "query", sequential = true)
public class FetchPlanTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public FetchPlanTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  public void beforeMeth() throws Exception {
    database.getMetadata().getSchema().createClass("FetchClass");

    database
        .getMetadata()
        .getSchema()
        .createClass("SecondFetchClass")
        .createProperty("surname", OType.STRING)
        .setMandatory(true);
    database.getMetadata().getSchema().createClass("OutInFetchClass");
    ODocument singleLinked = new ODocument();
    database.save(singleLinked);
    ODocument doc = new ODocument("FetchClass");
    doc.field("name", "first");
    database.save(doc);
    ODocument doc1 = new ODocument("FetchClass");
    doc1.field("name", "second");
    doc1.field("linked", singleLinked);
    database.save(doc1);
    ODocument doc2 = new ODocument("FetchClass");
    doc2.field("name", "third");
    List<ODocument> linkList = new ArrayList<ODocument>();
    linkList.add(doc);
    linkList.add(doc1);
    doc2.field("linkList", linkList);
    doc2.field("linked", singleLinked);
    Set<ODocument> linkSet = new HashSet<ODocument>();
    linkSet.add(doc);
    linkSet.add(doc1);
    doc2.field("linkSet", linkSet);
    database.save(doc2);

    ODocument doc3 = new ODocument("FetchClass");
    doc3.field("name", "forth");
    doc3.field("ref", doc2);
    doc3.field("linkSet", linkSet);
    doc3.field("linkList", linkList);
    database.save(doc3);

    ODocument doc4 = new ODocument("SecondFetchClass");
    doc4.field("name", "fifth");
    doc4.field("surname", "test");
    database.save(doc4);

    ODocument doc5 = new ODocument("SecondFetchClass");
    doc5.field("name", "sixth");
    doc5.field("surname", "test");
    database.save(doc5);

    ODocument doc6 = new ODocument("OutInFetchClass");
    ORidBag out = new ORidBag();
    out.add(doc2);
    out.add(doc3);
    doc6.field("out_friend", out);
    ORidBag in = new ORidBag();
    in.add(doc4);
    in.add(doc5);
    doc6.field("in_friend", in);
    doc6.field("name", "myName");
    database.save(doc6);

    database.getLocalCache().clear();
  }

  @AfterMethod
  public void afterMeth() throws Exception {
    database.getMetadata().getSchema().dropClass("FetchClass");
    database.getMetadata().getSchema().dropClass("SecondFetchClass");
    database.getMetadata().getSchema().dropClass("OutInFetchClass");
  }

  @Test
  public void queryNoFetchPlan() {
    createBasicTestSchema();

    final long times = Orient.instance().getProfiler().getCounter("Cache.reused");

    database.getLocalCache().clear();
    List<ODocument> resultset =
        database.query(new OSQLSynchQuery<ODocument>("select * from FetchClass"));
    Assert.assertEquals(Orient.instance().getProfiler().getCounter("Cache.reused"), times);

    ORID linked;
    for (ODocument d : resultset) {
      linked = ((ORID) d.field("linked", ORID.class));
      if (linked != null) Assert.assertNull(database.getLocalCache().findRecord(linked));
    }
  }

  @Test
  public void queryWithFetchPlan() {
    final long times = Orient.instance().getProfiler().getCounter("Cache.reused");
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from FetchClass").setFetchPlan("*:-1"));
    Assert.assertEquals(Orient.instance().getProfiler().getCounter("Cache.reused"), times);

    ODocument linked;
    for (ODocument d : resultset) {
      linked = ((ODocument) d.field("linked"));
      if (linked != null)
        Assert.assertNotNull(database.getLocalCache().findRecord(linked.getIdentity()));
    }
  }

  @Test(enabled = false)
  public void queryWithExcludeFetchPlan() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>(
                    "select * from FetchClass where name is not null and linkSet is not null")
                .setFetchPlan("linkSet:-2 name:-1"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      Assert.assertNull(d.field("linkSet"));
    }
  }

  @Test(enabled = false)
  public void queryWithExcludeWildcardFetchPlan() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>(
                    "select * from FetchClass where name is not null and linkSet is not null")
                .setFetchPlan("link*:-2 *:1"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      Assert.assertNull(d.field("linkSet"));
      Assert.assertNull(d.field("linkList"));
    }
  }

  @Test(enabled = false)
  public void queryOutInWithExcludeWildcardFetchPlan() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from OutInFetchClass ")
                .setFetchPlan("*:1 out_*:-2 in_*:-2"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      Assert.assertNull(d.field("out_friend"));
      Assert.assertNull(d.field("in_friend"));
    }
  }

  @Test(enabled = false)
  public void queryWithFullFetchPlan() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select * from FetchClass where name is not null and linkSet is not null"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      Assert.assertNotNull(d.field("linkSet"));
      Assert.assertNotNull(d.field("linkList"));
    }
  }

  @Test(enabled = false)
  public void queryFetchPlanDepth() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from FetchClass where name = 'forth' ")
                .setFetchPlan("ref:-1 ref.link*:-2"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      ODocument ref = d.field("ref");
      Assert.assertNotNull(ref.field("name"));
      Assert.assertNull(ref.field("linkSet"));
      Assert.assertNull(ref.field("linkList"));
    }
  }

  @Test(enabled = false)
  public void queryUpdateReadedWithPlanDepth() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from FetchClass where name = 'forth' ")
                .setFetchPlan("ref:-1 ref.link*:-2"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      ODocument ref = d.field("ref");
      Assert.assertNotNull(ref.field("name"));
      Assert.assertNull(ref.field("linkSet"));
      Assert.assertNull(ref.field("linkList"));
      d.field("name2", "value");
      database.save(d);
    }
    database.getLocalCache().clear();
    resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from FetchClass where name = 'forth' "));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      Assert.assertNotNull(d.field("name2"));
      ODocument ref = d.field("ref");
      Assert.assertNotNull(ref.field("name"));
      Assert.assertNotNull(ref.field("linkSet"));
      Assert.assertNotNull(ref.field("linkList"));
    }
  }

  @Test(enabled = false)
  public void queryUpdateConstraintReadedWithFetchPlan() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from SecondFetchClass where name = 'sixth'")
                .setFetchPlan("name:-1 surname:-2"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      Assert.assertNull(d.field("surname"));
      d.field("name", "sixth1");
      database.save(d);
    }
    database.getLocalCache().clear();
    resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from SecondFetchClass where name = 'sixth1'"));

    Assert.assertEquals(resultset.size(), 0);
  }

  @Test(enabled = false)
  public void queryDeleteReadedWithFetchPlan() {
    List<ODocument> resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from SecondFetchClass where name = 'fifth'")
                .setFetchPlan("*:1 surname:-2"));

    for (ODocument d : resultset) {
      Assert.assertNotNull(d.field("name"));
      Assert.assertNull(d.field("surname"));
      database.delete(d);
    }
    database.getLocalCache().clear();
    resultset =
        database.query(
            new OSQLSynchQuery<ODocument>("select * from SecondFetchClass where name = 'fifth'"));

    Assert.assertEquals(resultset.size(), 0);
  }
}
