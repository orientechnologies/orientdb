package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexTxAwareMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.*;

import java.util.Collection;

@Test
public class IndexTxAwareMultiValueGetTest extends DocumentDBBaseTest {

  private static final String CLASS_NAME = "idxTxAwareMultiValueGetTest";
  private static final String FIELD_NAME = "value";
  private static final String INDEX_NAME = "idxTxAwareMultiValueGetTestIndex";

  @Parameters(value = "url")
  public IndexTxAwareMultiValueGetTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(FIELD_NAME, OType.INTEGER);
    cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.NOTUNIQUE, FIELD_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.getMetadata().getSchema().getClass(CLASS_NAME).truncate();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 2);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testRemove() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final ODocument docOne = new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    final ODocument docTwo = new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    docOne.delete();
    docTwo.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertTrue(((OIndexTxAwareMultiValue) index).get(1).isEmpty());
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testRemoveOne() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final ODocument document = new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.begin();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 2);
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(2).size(), 1);
  }

  @Test
  public void testMultiPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final ODocument document = new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);

    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    document.save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);
    database.commit();

    Assert.assertEquals(((OIndexTxAwareMultiValue) index).get(1).size(), 1);
  }

  @Test
  public void testPutAfterTransaction() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Collection<?> resultOne = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(resultOne.size(), 1);
    database.commit();

    new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();

    resultOne = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(resultOne.size(), 2);
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final ODocument document = new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Collection<?> result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertTrue(result.isEmpty());

    database.commit();

    result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testPutAfterRemove() {
    if (((ODatabaseInternal) database).getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);
    Assert.assertTrue(index instanceof OIndexTxAwareMultiValue);

    final ODocument document = new ODocument(CLASS_NAME).field(FIELD_NAME, 1).save();
    document.removeField(FIELD_NAME);
    document.save();

    document.field(FIELD_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    Collection<?> result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(result.size(), 1);

    database.commit();

    result = ((OIndexTxAwareMultiValue) index).get(1);
    Assert.assertEquals(result.size(), 1);
  }
}
