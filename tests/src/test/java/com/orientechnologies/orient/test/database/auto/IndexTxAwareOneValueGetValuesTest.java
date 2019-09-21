package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexTxAwareOneValue;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Test
public class IndexTxAwareOneValueGetValuesTest extends DocumentDBBaseTest {
  private static final String CLASS_NAME = "IndexTxAwareOneValueGetValuesTest";
  private static final String FIELD_NAME = "value";
  private static final String INDEX_NAME = "IndexTxAwareOneValueGetValuesTest";

  @Parameters(value = "url")
  public IndexTxAwareOneValueGetValuesTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass cls = schema.createClass(CLASS_NAME);
    cls.createProperty(FIELD_NAME, OType.INTEGER);
    cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.UNIQUE, FIELD_NAME);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final OClass cls = database.getMetadata().getSchema().getClass(CLASS_NAME);
    cls.truncate();
  }

  @Test
  public void testPut() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();
    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<OIdentifiable> resultOne = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 3).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<OIdentifiable> resultTwo = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 3);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> resultThree = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemove() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    Set<OIdentifiable> resultOne = new HashSet<>();
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    ((OIdentifiable) index.get(1)).getRecord().delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    Set<OIdentifiable> resultTwo = new HashSet<>();
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> resultThree = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultThree);
    Assert.assertEquals(resultThree.size(), 2);
  }

  @Test
  public void testRemoveAndPut() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> resultOne = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultOne);
    Assert.assertEquals(resultOne.size(), 2);

    database.begin();

    ((OIdentifiable) index.get(1)).getRecord().delete();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> resultTwo = new HashSet<>();
    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, resultTwo);
    Assert.assertEquals(resultTwo.size(), 2);

    database.rollback();
  }

  @Test
  public void testMultiPut() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    final ODocument document = new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testPutAfterTransaction() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);
    database.commit();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 3).save();

    cursor = index.iterateEntries(Arrays.asList(1, 2, 3), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 3);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    ((OIdentifiable) index.get(1)).getRecord().delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testRemoveAllWithinTransaction() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    ((OIdentifiable) index.get(1)).getRecord().delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPutAfterRemove() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex<?> index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareOneValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    ((OIdentifiable) index.get(1)).getRecord().delete();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Set<OIdentifiable> result = new HashSet<>();
    OIndexCursor cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);

    Assert.assertEquals(result.size(), 2);

    database.commit();

    cursor = index.iterateEntries(Arrays.asList(1, 2), true);
    cursorToSet(cursor, result);
    Assert.assertEquals(result.size(), 2);
  }

  private void cursorToSet(OIndexCursor cursor, Set<OIdentifiable> result) {
    result.clear();
    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      result.add(entry.getValue());
      entry = cursor.nextEntry();
    }
  }
}
