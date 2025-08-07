package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexTxAwareMultiValueGetValuesTest extends DocumentDBBaseTest {
  private static final String CLASS_NAME = "idxTxAwareMultiValueGetValuesTest";
  private static final String PROPERTY_NAME = "value";
  private static final String INDEX = "idxTxAwareMultiValueGetValuesTestIndex";

  @Parameters(value = "url")
  public IndexTxAwareMultiValueGetValuesTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(PROPERTY_NAME, OType.INTEGER);
    cls.createIndex(INDEX, OClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.getMetadata().getSchema().getClass(CLASS_NAME).truncate();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);

    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 4);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemove() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument documentOne = database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    ODocument documentTwo = database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    database.delete(documentOne);
    database.delete(documentTwo);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testRemoveOne() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument documentOne = database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultOne = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultOne);
    Assert.assertEquals(resultOne.size(), 3);

    database.begin();

    database.delete(documentOne);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultTwo = new HashSet<>();
    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> resultThree = new HashSet<>();
    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, resultThree);
    Assert.assertEquals(resultThree.size(), 3);
  }

  @Test
  public void testMultiPut() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    final ODocument document = database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));

    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);
    database.save(document);

    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);

    database.commit();

    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testPutAfterTransaction() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
    database.commit();

    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));

    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    database.delete(document);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    database.delete(document);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2));

    document.removeField(PROPERTY_NAME);
    database.save(document);

    database.save(document.field(PROPERTY_NAME, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    Set<OIdentifiable> result = new HashSet<>();
    Stream<ORawPair<Object, ORID>> stream =
        index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    stream = index.getInternal().streamEntries(Arrays.asList(1, 2), true);
    streamToSet(stream, result);
    Assert.assertEquals(result.size(), 2);
  }

  private static void streamToSet(
      Stream<ORawPair<Object, ORID>> stream, Set<OIdentifiable> result) {
    result.clear();
    result.addAll(stream.map((entry) -> entry.second).collect(Collectors.toSet()));
  }
}
