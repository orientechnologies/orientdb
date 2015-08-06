/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.List;

/**
 * @author Luigi Dell'Aquila l.dellaquila-at-orientechnologies.com
 */
public class PolymorphicQueryTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public PolymorphicQueryTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    database.command(new OCommandSQL("create class IndexInSubclassesTestBase")).execute();
    database.command(new OCommandSQL("create property IndexInSubclassesTestBase.name string")).execute();

    database.command(new OCommandSQL("create class IndexInSubclassesTestChild1 extends IndexInSubclassesTestBase")).execute();
    database.command(
        new OCommandSQL("create index IndexInSubclassesTestChild1.name on IndexInSubclassesTestChild1 (name) notunique")).execute();

    database.command(new OCommandSQL("create class IndexInSubclassesTestChild2 extends IndexInSubclassesTestBase")).execute();
    database.command(
        new OCommandSQL("create index IndexInSubclassesTestChild2.name on IndexInSubclassesTestChild2 (name) notunique")).execute();

    database.command(new OCommandSQL("create class IndexInSubclassesTestBaseFail")).execute();
    database.command(new OCommandSQL("create property IndexInSubclassesTestBaseFail.name string")).execute();

    database.command(new OCommandSQL("create class IndexInSubclassesTestChild1Fail extends IndexInSubclassesTestBaseFail"))
        .execute();
    // database.command(
    // new OCommandSQL("create index IndexInSubclassesTestChild1Fail.name on IndexInSubclassesTestChild1Fail (name) notunique"))
    // .execute();

    database.command(new OCommandSQL("create class IndexInSubclassesTestChild2Fail extends IndexInSubclassesTestBaseFail"))
        .execute();
    database.command(
        new OCommandSQL("create index IndexInSubclassesTestChild2Fail.name on IndexInSubclassesTestChild2Fail (name) notunique"))
        .execute();

    database.command(new OCommandSQL("create class GenericCrash")).execute();
    database.command(new OCommandSQL("create class SpecificCrash extends GenericCrash")).execute();

  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final OSchema schema = database.getMetadata().getSchema();
    schema.reload();
    database.getStorage().reload();

    database.command(new OCommandSQL("delete from IndexInSubclassesTestBase")).execute();
    database.command(new OCommandSQL("delete from IndexInSubclassesTestChild1")).execute();
    database.command(new OCommandSQL("delete from IndexInSubclassesTestChild2")).execute();

    database.command(new OCommandSQL("delete from IndexInSubclassesTestBaseFail")).execute();
    database.command(new OCommandSQL("delete from IndexInSubclassesTestChild1Fail")).execute();
    database.command(new OCommandSQL("delete from IndexInSubclassesTestChild2Fail")).execute();

  }

  @Test
  public void testSubclassesIndexes() throws Exception {
    database.begin();

    OProfiler profiler = Orient.instance().getProfiler();

    long indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long indexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");

    if (indexUsage < 0) {
      indexUsage = 0;
    }

    if (indexUsageReverted < 0) {
      indexUsageReverted = 0;
    }

    profiler.startRecording();
    for (int i = 0; i < 10000; i++) {

      final ODocument doc1 = new ODocument("IndexInSubclassesTestChild1");
      doc1.field("name", "name" + i);
      doc1.save();

      final ODocument doc2 = new ODocument("IndexInSubclassesTestChild2");
      doc2.field("name", "name" + i);
      doc2.save();
      if (i % 100 == 0) {
        database.commit();
      }
    }
    database.commit();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select from IndexInSubclassesTestBase where name > 'name9995' and name < 'name9999' order by name ASC"));
    Assert.assertEquals(result.size(), 6);
    String lastName = result.get(0).field("name");

    for (int i = 1; i < result.size(); i++) {
      ODocument current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) <= 0);
      lastName = currentName;
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), indexUsage + 2);
    long reverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");
    Assert.assertEquals(reverted < 0 ? 0 : reverted, indexUsageReverted);

    result = database.query(new OSQLSynchQuery<ODocument>(
        "select from IndexInSubclassesTestBase where name > 'name9995' and name < 'name9999' order by name DESC"));
    Assert.assertEquals(result.size(), 6);
    lastName = result.get(0).field("name");
    for (int i = 1; i < result.size(); i++) {
      ODocument current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) >= 0);
      lastName = currentName;
    }
    profiler.stopRecording();
  }

  @Test
  public void testBaseWithoutIndexAndSubclassesIndexes() throws Exception {
    database.begin();

    OProfiler profiler = Orient.instance().getProfiler();

    long indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long indexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");

    if (indexUsage < 0) {
      indexUsage = 0;
    }

    if (indexUsageReverted < 0) {
      indexUsageReverted = 0;
    }

    profiler.startRecording();
    for (int i = 0; i < 10000; i++) {
      final ODocument doc0 = new ODocument("IndexInSubclassesTestBase");
      doc0.field("name", "name" + i);
      doc0.save();

      final ODocument doc1 = new ODocument("IndexInSubclassesTestChild1");
      doc1.field("name", "name" + i);
      doc1.save();

      final ODocument doc2 = new ODocument("IndexInSubclassesTestChild2");
      doc2.field("name", "name" + i);
      doc2.save();
      if (i % 100 == 0) {
        database.commit();
      }
    }
    database.commit();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select from IndexInSubclassesTestBase where name > 'name9995' and name < 'name9999' order by name ASC"));
    Assert.assertTrue(result.size() == 9);
    String lastName = result.get(0).field("name");
    for (int i = 1; i < result.size(); i++) {
      ODocument current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) <= 0);
      lastName = currentName;
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), indexUsage + 2);

    long reverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");
    Assert.assertEquals(reverted < 0 ? 0 : reverted, indexUsageReverted);

    result = database.query(new OSQLSynchQuery<ODocument>(
        "select from IndexInSubclassesTestBase where name > 'name9995' and name < 'name9999' order by name DESC"));
    Assert.assertTrue(result.size() == 9);
    lastName = result.get(0).field("name");
    for (int i = 1; i < result.size(); i++) {
      ODocument current = result.get(i);
      String currentName = current.field("name");
      Assert.assertTrue(lastName.compareTo(currentName) >= 0);
      lastName = currentName;
    }
    profiler.stopRecording();
  }

  @Test
  public void testSubclassesIndexesFailed() throws Exception {
    database.begin();

    OProfiler profiler = Orient.instance().getProfiler();
    profiler.startRecording();

    for (int i = 0; i < 10000; i++) {

      final ODocument doc1 = new ODocument("IndexInSubclassesTestChild1Fail");
      doc1.field("name", "name" + i);
      doc1.save();

      final ODocument doc2 = new ODocument("IndexInSubclassesTestChild2Fail");
      doc2.field("name", "name" + i);
      doc2.save();
      if (i % 100 == 0) {
        database.commit();
      }
    }
    database.commit();

    long indexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long indexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");

    if (indexUsage < 0) {
      indexUsage = 0;
    }

    if (indexUsageReverted < 0) {
      indexUsageReverted = 0;
    }

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select from IndexInSubclassesTestBaseFail where name > 'name9995' and name < 'name9999' order by name ASC"));
    Assert.assertTrue(result.size() == 6);

    long lastIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long lastIndexUsageReverted = profiler.getCounter("db.demo.query.indexUseAttemptedAndReverted");
    if (lastIndexUsage < 0) {
      lastIndexUsage = 0;
    }

    if (lastIndexUsageReverted < 0) {
      lastIndexUsageReverted = 0;
    }

    Assert.assertEquals(lastIndexUsage - indexUsage, lastIndexUsageReverted - indexUsageReverted);

    profiler.stopRecording();
  }

  @Test
  public void testIteratorOnSubclassWithoutValues() {
    for (int i = 0; i < 2; i++) {
      final ODocument doc1 = new ODocument("GenericCrash");
      doc1.field("name", "foo");
      doc1.save();
    }

    // crashed with OIOException, issue #3632
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "SELECT FROM GenericCrash WHERE @class='GenericCrash' ORDER BY @rid DESC"));

    Assert.assertEquals(result.size(), 2);
    for (ODocument doc : result) {
      Assert.assertEquals(doc.field("name"), "foo");
    }

  }
}
