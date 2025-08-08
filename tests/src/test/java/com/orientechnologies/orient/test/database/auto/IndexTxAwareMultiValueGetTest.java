package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

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
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));

    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.begin();

    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 2));

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 2);
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testRemove() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final ODocument docOne = database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));
    final ODocument docTwo = database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));

    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.begin();

    database.delete(docOne);
    database.delete(docTwo);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testRemoveOne() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final ODocument document = database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));
    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));

    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 2));

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.begin();

    database.delete(document);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 2);
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testMultiPut() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final ODocument document = database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 1);
    }

    document.field(FIELD_NAME, 0);
    document.field(FIELD_NAME, 1);
    database.save(document);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    database.commit();

    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }

  @Test
  public void testPutAfterTransaction() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 1);
    }
    database.commit();

    database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));

    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 2);
    }
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final ODocument document = database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));
    database.delete(document);

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    database.commit();

    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 0);
    }
  }

  @Test
  public void testPutAfterRemove() {
    if (database.isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    final ODocument document = database.save(new ODocument(CLASS_NAME).field(FIELD_NAME, 1));
    document.removeField(FIELD_NAME);
    database.save(document);

    database.save(document.field(FIELD_NAME, 1));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 1);
    }

    database.commit();

    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertEquals(stream.count(), 1);
    }
  }
}
