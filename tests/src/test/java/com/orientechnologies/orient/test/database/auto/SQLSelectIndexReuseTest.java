package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLSelectIndexReuseTest extends AbstractIndexReuseTest {
  @Parameters(value = "url")
  public SQLSelectIndexReuseTest(@Optional final String iURL) {
    super(iURL);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    if (database.isClosed()) {
      database.open("admin", "admin");
    }

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass("sqlSelectIndexReuseTestClass");

    oClass.createProperty("prop1", OType.INTEGER);
    oClass.createProperty("prop2", OType.INTEGER);
    oClass.createProperty("prop3", OType.INTEGER);
    oClass.createProperty("prop4", OType.INTEGER);
    oClass.createProperty("prop5", OType.INTEGER);
    oClass.createProperty("prop6", OType.INTEGER);
    oClass.createProperty("prop7", OType.STRING);
    oClass.createProperty("prop8", OType.INTEGER);
    oClass.createProperty("prop9", OType.INTEGER);

    oClass.createProperty("fEmbeddedMap", OType.EMBEDDEDMAP, OType.INTEGER);
    oClass.createProperty("fEmbeddedMapTwo", OType.EMBEDDEDMAP, OType.INTEGER);

    oClass.createProperty("fLinkMap", OType.LINKMAP);

    oClass.createProperty("fEmbeddedList", OType.EMBEDDEDLIST, OType.INTEGER);
    oClass.createProperty("fEmbeddedListTwo", OType.EMBEDDEDLIST, OType.INTEGER);

    oClass.createProperty("fLinkList", OType.LINKLIST);

    oClass.createProperty("fEmbeddedSet", OType.EMBEDDEDSET, OType.INTEGER);
    oClass.createProperty("fEmbeddedSetTwo", OType.EMBEDDEDSET, OType.INTEGER);

    oClass.createIndex("indexone", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");
    oClass.createIndex("indextwo", OClass.INDEX_TYPE.UNIQUE, "prop3");
    oClass.createIndex("indexthree", OClass.INDEX_TYPE.NOTUNIQUE, "prop1", "prop2", "prop4");
    oClass.createIndex("indexfour", OClass.INDEX_TYPE.NOTUNIQUE, "prop4", "prop1", "prop3");
    oClass.createIndex("indexfive", OClass.INDEX_TYPE.NOTUNIQUE, "prop6", "prop1", "prop3");

    oClass.createIndex(
        "sqlSelectIndexReuseTestEmbeddedMapByKey", OClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedMap");
    oClass.createIndex(
        "sqlSelectIndexReuseTestEmbeddedMapByValue",
        OClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedMap by value");
    oClass.createIndex(
        "sqlSelectIndexReuseTestEmbeddedList", OClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedList");

    oClass.createIndex(
        "sqlSelectIndexReuseTestEmbeddedMapByKeyProp8",
        OClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedMapTwo",
        "prop8");
    oClass.createIndex(
        "sqlSelectIndexReuseTestEmbeddedMapByValueProp8",
        OClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedMapTwo by value",
        "prop8");

    oClass.createIndex(
        "sqlSelectIndexReuseTestEmbeddedSetProp8",
        OClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedSetTwo",
        "prop8");
    oClass.createIndex(
        "sqlSelectIndexReuseTestProp9EmbeddedSetProp8",
        OClass.INDEX_TYPE.NOTUNIQUE,
        "prop9",
        "fEmbeddedSetTwo",
        "prop8");

    oClass.createIndex(
        "sqlSelectIndexReuseTestEmbeddedListTwoProp8",
        OClass.INDEX_TYPE.NOTUNIQUE,
        "fEmbeddedListTwo",
        "prop8");

    final String fullTextIndexStrings[] = {
      "Alice : What is the use of a book, without pictures or conversations?",
      "Rabbit : Oh my ears and whiskers, how late it's getting!",
      "Alice : If it had grown up, it would have made a dreadfully ugly child; but it makes rather a handsome pig, I think",
      "The Cat : We're all mad here.",
      "The Hatter : Why is a raven like a writing desk?",
      "The Hatter : Twinkle, twinkle, little bat! How I wonder what you're at.",
      "The Queen : Off with her head!",
      "The Duchess : Tut, tut, child! Everything's got a moral, if only you can find it.",
      "The Duchess : Take care of the sense, and the sounds will take care of themselves.",
      "The King : Begin at the beginning and go on till you come to the end: then stop."
    };

    for (int i = 0; i < 10; i++) {
      final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

      embeddedMap.put("key" + (i * 10 + 1), i * 10 + 1);
      embeddedMap.put("key" + (i * 10 + 2), i * 10 + 2);
      embeddedMap.put("key" + (i * 10 + 3), i * 10 + 3);
      embeddedMap.put("key" + (i * 10 + 4), i * 10 + 1);

      final List<Integer> embeddedList = new ArrayList<Integer>(3);
      embeddedList.add(i * 3);
      embeddedList.add(i * 3 + 1);
      embeddedList.add(i * 3 + 2);

      final Set<Integer> embeddedSet = new HashSet<Integer>();
      embeddedSet.add(i * 10);
      embeddedSet.add(i * 10 + 1);
      embeddedSet.add(i * 10 + 2);

      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument("sqlSelectIndexReuseTestClass");
        document.field("prop1", i);
        document.field("prop2", j);
        document.field("prop3", i * 10 + j);

        document.field("prop4", i);
        document.field("prop5", i);

        document.field("prop6", j);

        document.field("prop7", fullTextIndexStrings[i]);

        document.field("prop8", j);

        document.field("prop9", j % 2);
        document.field("fEmbeddedMap", embeddedMap);

        document.field("fEmbeddedMapTwo", embeddedMap);

        document.field("fEmbeddedList", embeddedList);
        document.field("fEmbeddedListTwo", embeddedList);

        document.field("fEmbeddedSet", embeddedSet);
        document.field("fEmbeddedSetTwo", embeddedSet);

        document.save();
      }
    }
    database.close();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (database.isClosed()) {
      database.open("admin", "admin");
    }

    database.command(new OCommandSQL("drop class sqlSelectIndexReuseTestClass")).execute();
    database.getMetadata().getSchema().reload();

    database.close();
    super.afterClass();
  }

  @Test
  public void testCompositeSearchEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 2"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchHasChainOperatorsEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1.asInteger() = 1 and prop2 = 2"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchEqualsOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.1"), oldcompositeIndexUsed21 + 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldWithLimit() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop3 = 18 limit 1"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = new ODocument();
    document.field("prop1", 1);
    document.field("prop3", 18);

    Assert.assertEquals(containsDocument(result, document), 1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.1"), oldcompositeIndexUsed21 + 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldMapIndexByKey() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }

    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where fEmbeddedMapTwo containsKey 'key11'"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop8", 1);
      document.field("fEmbeddedMapTwo", embeddedMap);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.1"), oldcompositeIndexUsed21 + 1);
  }

  @Test
  public void testCompositeSearchEqualsMapIndexByKey() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed22 == -1) {
      oldcompositeIndexUsed22 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass "
                        + "where prop8 = 1 and fEmbeddedMapTwo containsKey 'key11'"))
            .execute();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    Assert.assertEquals(result.size(), 1);

    final ODocument document = new ODocument();
    document.field("prop8", 1);
    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22 + 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldMapIndexByValue() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }
    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass "
                        + "where fEmbeddedMapTwo containsValue 22"))
            .execute();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key21", 21);
    embeddedMap.put("key22", 22);
    embeddedMap.put("key23", 23);
    embeddedMap.put("key24", 21);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop8", i);
      document.field("fEmbeddedMapTwo", embeddedMap);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.1"), oldcompositeIndexUsed21 + 1);
  }

  @Test
  public void testCompositeSearchEqualsMapIndexByValue() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }
    if (oldcompositeIndexUsed22 == -1) {
      oldcompositeIndexUsed22 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass "
                        + "where prop8 = 1 and fEmbeddedMapTwo containsValue 22"))
            .execute();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key21", 21);
    embeddedMap.put("key22", 22);
    embeddedMap.put("key23", 23);
    embeddedMap.put("key24", 21);

    Assert.assertEquals(result.size(), 1);

    final ODocument document = new ODocument();
    document.field("prop8", 1);
    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22 + 1);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedSetIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed22 == -1) {
      oldcompositeIndexUsed22 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass "
                        + "where prop8 = 1 and fEmbeddedSetTwo contains 12"))
            .execute();

    final Set<Integer> embeddedSet = new HashSet<Integer>();
    embeddedSet.add(10);
    embeddedSet.add(11);
    embeddedSet.add(12);

    Assert.assertEquals(result.size(), 1);

    final ODocument document = new ODocument();
    document.field("prop8", 1);
    document.field("fEmbeddedSet", embeddedSet);

    Assert.assertEquals(containsDocument(result, document), 1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22 + 1);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedSetInMiddleIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }

    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }

    if (oldcompositeIndexUsed33 == -1) {
      oldcompositeIndexUsed33 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass "
                        + "where prop9 = 0 and fEmbeddedSetTwo contains 92 and prop8 > 2"))
            .execute();

    final Set<Integer> embeddedSet = new HashSet<Integer>(3);
    embeddedSet.add(90);
    embeddedSet.add(91);
    embeddedSet.add(92);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i < 3; i++) {
      final ODocument document = new ODocument();
      document.field("prop8", i * 2 + 4);
      document.field("prop9", 0);
      document.field("fEmbeddedSet", embeddedSet);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3.3"), oldcompositeIndexUsed33 + 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldEmbeddedListIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed21 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.1");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }

    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    if (oldcompositeIndexUsed21 == -1) {
      oldcompositeIndexUsed21 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where fEmbeddedListTwo contains 4"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(3);
    embeddedList.add(4);
    embeddedList.add(5);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop8", i);
      document.field("fEmbeddedListTwo", embeddedList);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.1"), oldcompositeIndexUsed21 + 1);
  }

  @Test
  public void testCompositeSearchEqualsEmbeddedListIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");
    long oldcompositeIndexUsed22 = profiler.getCounter("db.demo.query.compositeIndexUsed.2.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }
    if (oldcompositeIndexUsed22 == -1) {
      oldcompositeIndexUsed22 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where"
                        + " prop8 = 1 and fEmbeddedListTwo contains 4"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(3);
    embeddedList.add(4);
    embeddedList.add(5);

    final ODocument document = new ODocument();
    document.field("prop8", 1);
    document.field("fEmbeddedListTwo", embeddedList);

    Assert.assertEquals(containsDocument(result, document), 1);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2.2"), oldcompositeIndexUsed22 + 1);
  }

  @Test
  public void testNoCompositeSearchEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 = 1"))
            .execute();

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", i);
      document.field("prop2", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchEqualsWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 = ?"))
            .execute(1, 2);

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchEqualsOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = ?"))
            .execute(1);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testNoCompositeSearchEqualsWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 = ?"))
            .execute(1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", i);
      document.field("prop2", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }
  }

  @Test
  public void testCompositeSearchGT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 > 2"))
            .execute();

    Assert.assertEquals(result.size(), 7);

    for (int i = 3; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 > 7"))
            .execute();

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 > 7"))
            .execute();

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchGTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 > ?"))
            .execute(1, 2);

    Assert.assertEquals(result.size(), 7);

    for (int i = 3; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 > ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 > ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 20);

    for (int i = 8; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchGTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 >= 2"))
            .execute();

    Assert.assertEquals(result.size(), 8);

    for (int i = 2; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTQOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 >= 7"))
            .execute();

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 >= 7"))
            .execute();

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchGTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 >= ?"))
            .execute(1, 2);

    Assert.assertEquals(result.size(), 8);

    for (int i = 2; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTQOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 >= ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 >= ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 30);

    for (int i = 7; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 <= 2"))
            .execute();

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 2; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTQOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 <= 7"))
            .execute();

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 <= 7"))
            .execute();

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 <= ?"))
            .execute(1, 2);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 2; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTQOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 <= ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 <= ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 80);

    for (int i = 0; i <= 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 < 2"))
            .execute();

    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 < 7"))
            .execute();

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 < 7"))
            .execute();

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchLTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 < ?"))
            .execute(1, 2);

    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 < ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 < ?"))
            .execute(7);

    Assert.assertEquals(result.size(), 70);

    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchBetween() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 between 1 and 3"))
            .execute();

    Assert.assertEquals(result.size(), 3);

    for (int i = 1; i <= 3; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchBetweenOneField() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 between 1 and 3"))
            .execute();

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 between 1 and 3"))
            .execute();

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testCompositeSearchBetweenWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 between ? and ?"))
            .execute(1, 3);

    Assert.assertEquals(result.size(), 3);

    for (int i = 1; i <= 3; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchBetweenOneFieldWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 between ? and ?"))
            .execute(1, 3);

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", i);
        document.field("prop2", j);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearchWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop2 between ? and ?"))
            .execute(1, 3);

    Assert.assertEquals(result.size(), 30);

    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < 10; j++) {
        final ODocument document = new ODocument();
        document.field("prop1", j);
        document.field("prop2", i);

        Assert.assertEquals(containsDocument(result, document), 1);
      }
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage);
  }

  @Test
  public void testSingleSearchEquals() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 = 1"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 1);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchEqualsWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 = ?"))
            .execute(1);

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 1);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchGT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 > 90"))
            .execute();

    Assert.assertEquals(result.size(), 9);

    for (int i = 91; i < 100; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchGTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 > ?"))
            .execute(90);

    Assert.assertEquals(result.size(), 9);

    for (int i = 91; i < 100; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchGTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 >= 90"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    for (int i = 90; i < 100; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchGTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 >= ?"))
            .execute(90);

    Assert.assertEquals(result.size(), 10);

    for (int i = 90; i < 100; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLTQ() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 <= 10"))
            .execute();

    Assert.assertEquals(result.size(), 11);

    for (int i = 0; i <= 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLTQWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 <= ?"))
            .execute(10);

    Assert.assertEquals(result.size(), 11);

    for (int i = 0; i <= 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLT() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 < 10"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchLTWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 < ?"))
            .execute(10);

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchBetween() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 between 1 and 10"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    for (int i = 1; i <= 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchBetweenWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 between ? and ?"))
            .execute(1, 10);

    Assert.assertEquals(result.size(), 10);

    for (int i = 1; i <= 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchIN() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 in [0, 5, 10]"))
            .execute();

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 10; i += 5) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testSingleSearchINWithArgs() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop3 in [?, ?, ?]"))
            .execute(0, 5, 10);

    Assert.assertEquals(result.size(), 3);

    for (int i = 0; i <= 10; i += 5) {
      final ODocument document = new ODocument();
      document.field("prop3", i);
      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testMostSpecificOnesProcessedFirst() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and prop3 = 11"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 11);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testTripleSearch() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and prop4 >= 1"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3 + 1);
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexFirstCase() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and prop5 >= 1"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop5").intValue(), 1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexSecondCase() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop4 >= 1"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);
      document.field("prop4", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testTripleSearchLastFieldInIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop4 = 1"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    for (int i = 0; i < 10; i++) {
      final ODocument document = new ODocument();
      document.field("prop1", 1);
      document.field("prop2", i);
      document.field("prop4", 1);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3 + 1);
  }

  @Test
  public void testTripleSearchLastFieldsCanNotBeMerged() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop6 <= 1 and prop4 < 1"))
            .execute();

    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final ODocument document = new ODocument();
      document.field("prop6", i);
      document.field("prop4", 0);

      Assert.assertEquals(containsDocument(result, document), 1);
    }

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3 + 1);
  }

  @Test
  public void testLastFieldNotCompatibleOperator() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 + 1 = 3"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testEmbeddedMapByKeyIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where fEmbeddedMap containskey 'key12'"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    final ODocument document = new ODocument();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 10);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testEmbeddedMapBySpecificKeyIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where ( fEmbeddedMap containskey 'key12' ) and ( fEmbeddedMap['key12'] = 12 )"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    final ODocument document = new ODocument();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 10);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testEmbeddedMapByValueIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where fEmbeddedMap containsvalue 11"))
            .execute();

    Assert.assertEquals(result.size(), 10);

    final ODocument document = new ODocument();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put("key11", 11);
    embeddedMap.put("key12", 12);
    embeddedMap.put("key13", 13);
    embeddedMap.put("key14", 11);

    document.field("fEmbeddedMap", embeddedMap);

    Assert.assertEquals(containsDocument(result, document), 10);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testEmbeddedListIndexReuse() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where fEmbeddedList contains 7"))
            .execute();

    final List<Integer> embeddedList = new ArrayList<Integer>(3);
    embeddedList.add(6);
    embeddedList.add(7);
    embeddedList.add(8);

    final ODocument document = new ODocument();
    document.field("fEmbeddedList", embeddedList);

    Assert.assertEquals(containsDocument(result, document), 10);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2);
  }

  @Test
  public void testNotIndexOperatorFirstCase() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2  = 2 and ( prop4 = 3 or prop4 = 1 )"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testIndexUsedOnOrClause() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    if (oldIndexUsage < 0) oldIndexUsage = 0;

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where ( prop1 = 1 and prop2 = 2 ) or ( prop4  = 1 and prop6 = 2 )"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop2").intValue(), 2);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop6").intValue(), 2);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 2);
  }

  @Test
  public void testCompositeIndexEmptyResult() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed2 == -1) {
      oldcompositeIndexUsed2 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop1 = 1777 and prop2  = 2777"))
            .execute();

    Assert.assertEquals(result.size(), 0);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testReuseOfIndexOnSeveralClassesFields() {
    final OSchema schema = database.getMetadata().getSchema();
    final OClass superClass = schema.createClass("sqlSelectIndexReuseTestSuperClass");
    superClass.createProperty("prop0", OType.INTEGER);
    final OClass oClass = schema.createClass("sqlSelectIndexReuseTestChildClass", superClass);
    oClass.createProperty("prop1", OType.INTEGER);

    oClass.createIndex(
        "sqlSelectIndexReuseTestOnPropertiesFromClassAndSuperclass",
        OClass.INDEX_TYPE.UNIQUE,
        "prop0",
        "prop1");

    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed2 = profiler.getCounter("db.demo.query.compositeIndexUsed.2");

    final ODocument docOne = new ODocument("sqlSelectIndexReuseTestChildClass");
    docOne.field("prop0", 0);
    docOne.field("prop1", 1);
    docOne.save();

    final ODocument docTwo = new ODocument("sqlSelectIndexReuseTestChildClass");
    docTwo.field("prop0", 2);
    docTwo.field("prop1", 3);
    docTwo.save();

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestChildClass where prop0 = 0 and prop1 = 1"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.2"), oldcompositeIndexUsed2 + 1);
  }

  @Test
  public void testCountFunctionWithNotUniqueIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    OClass klazz =
        database.getMetadata().getSchema().getOrCreateClass("CountFunctionWithNotUniqueIndexTest");
    if (!klazz.existsProperty("a")) {
      klazz.createProperty("a", OType.STRING);
      klazz.createIndex("a", "NOTUNIQUE", "a");
    }

    database
        .<ODocument>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "a")
        .field("b", "b")
        .save();
    database
        .<ODocument>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "a")
        .field("b", "b")
        .save();
    database
        .<ODocument>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "a")
        .field("b", "e")
        .save();
    database
        .<ODocument>newInstance("CountFunctionWithNotUniqueIndexTest")
        .field("a", "c")
        .field("b", "c")
        .save();

    ODocument result =
        (ODocument)
            database
                .query(
                    new OSQLSynchQuery<ODocument>(
                        "select count(*) from CountFunctionWithNotUniqueIndexTest where a = 'a' and b = 'c'"))
                .get(0);

    Assert.assertEquals(result.<Object>field("count", Long.class), 0l);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  @Test
  public void testCountFunctionWithUniqueIndex() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");

    OClass klazz =
        database.getMetadata().getSchema().getOrCreateClass("CountFunctionWithUniqueIndexTest");
    if (!klazz.existsProperty("a")) {
      klazz.createProperty("a", OType.STRING);
      klazz.createIndex("testCountFunctionWithUniqueIndex", "NOTUNIQUE", "a");
    }

    database
        .<ODocument>newInstance("CountFunctionWithUniqueIndexTest")
        .field("a", "a")
        .field("b", "c")
        .save();
    database
        .<ODocument>newInstance("CountFunctionWithUniqueIndexTest")
        .field("a", "a")
        .field("b", "c")
        .save();
    database
        .<ODocument>newInstance("CountFunctionWithUniqueIndexTest")
        .field("a", "a")
        .field("b", "e")
        .save();
    ODocument doc =
        database
            .<ODocument>newInstance("CountFunctionWithUniqueIndexTest")
            .field("a", "a")
            .field("b", "b")
            .save();

    ODocument result =
        (ODocument)
            database
                .query(
                    new OSQLSynchQuery<ODocument>(
                        "select count(*) from CountFunctionWithUniqueIndexTest where a = 'a' and b = 'c'"))
                .get(0);

    Assert.assertEquals(result.<Object>field("count", Long.class), 2l);
    doc.delete();

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed);
  }

  private int containsDocument(final List<ODocument> docList, final ODocument document) {
    int count = 0;
    for (final ODocument docItem : docList) {
      boolean containsAllFields = true;
      for (final String fieldName : document.fieldNames()) {
        if (!document.<Object>field(fieldName).equals(docItem.<Object>field(fieldName))) {
          containsAllFields = false;
          break;
        }
      }
      if (containsAllFields) {
        count++;
      }
    }
    return count;
  }

  @Test
  public void testCompositeSearchIn1() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }
    if (oldcompositeIndexUsed33 == -1) {
      oldcompositeIndexUsed33 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop4 = 1 and prop1 = 1 and prop3 in [13, 113]"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 13);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3 + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3.3"), oldcompositeIndexUsed33 + 1);
  }

  @Test
  public void testCompositeSearchIn2() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }
    if (oldcompositeIndexUsed33 == -1) {
      oldcompositeIndexUsed33 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop4 = 1 and prop1 in [1, 2] and prop3 = 13"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 13);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3 + 1);
    Assert.assertTrue(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3.3") < oldcompositeIndexUsed33 + 1);
  }

  @Test
  public void testCompositeSearchIn3() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }
    if (oldcompositeIndexUsed33 == -1) {
      oldcompositeIndexUsed33 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop4 = 1 and prop1 in [1, 2] and prop3 in [13, 15]"))
            .execute();

    Assert.assertEquals(result.size(), 2);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertTrue(
        document.<Integer>field("prop3").equals(13) || document.<Integer>field("prop3").equals(15));

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3"), oldcompositeIndexUsed3 + 1);
    Assert.assertTrue(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3.3") < oldcompositeIndexUsed33 + 1);
  }

  @Test
  public void testCompositeSearchIn4() {
    long oldIndexUsage = profiler.getCounter("db.demo.query.indexUsed");
    long oldcompositeIndexUsed = profiler.getCounter("db.demo.query.compositeIndexUsed");
    long oldcompositeIndexUsed3 = profiler.getCounter("db.demo.query.compositeIndexUsed.3");
    long oldcompositeIndexUsed33 = profiler.getCounter("db.demo.query.compositeIndexUsed.3.3");

    if (oldIndexUsage == -1) {
      oldIndexUsage = 0;
    }
    if (oldcompositeIndexUsed == -1) {
      oldcompositeIndexUsed = 0;
    }
    if (oldcompositeIndexUsed3 == -1) {
      oldcompositeIndexUsed3 = 0;
    }
    if (oldcompositeIndexUsed33 == -1) {
      oldcompositeIndexUsed33 = 0;
    }

    final List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from sqlSelectIndexReuseTestClass where prop4 in [1, 2] and prop1 = 1 and prop3 = 13"))
            .execute();

    Assert.assertEquals(result.size(), 1);

    final ODocument document = result.get(0);
    Assert.assertEquals(document.<Integer>field("prop4").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop1").intValue(), 1);
    Assert.assertEquals(document.<Integer>field("prop3").intValue(), 13);

    Assert.assertEquals(profiler.getCounter("db.demo.query.indexUsed"), oldIndexUsage + 1);
    Assert.assertEquals(
        profiler.getCounter("db.demo.query.compositeIndexUsed"), oldcompositeIndexUsed + 1);
    Assert.assertTrue(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3") < oldcompositeIndexUsed3 + 1);
    Assert.assertTrue(
        profiler.getCounter("db.demo.query.compositeIndexUsed.3.3") < oldcompositeIndexUsed33 + 1);
  }
}
