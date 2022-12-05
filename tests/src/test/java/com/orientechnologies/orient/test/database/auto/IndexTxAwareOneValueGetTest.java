package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexTxAwareOneValueGetTest extends DocumentDBBaseTest {

  private static final String CLASS_NAME = "idxTxAwareOneValueGetTest";
  private static final String PROPERTY_NAME = "value";
  private static final String INDEX = "idxTxAwareOneValueGetTestIndex";

  @Parameters(value = "url")
  public IndexTxAwareOneValueGetTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    OClass cls = database.getMetadata().getSchema().createClass(CLASS_NAME);
    cls.createProperty(PROPERTY_NAME, OType.INTEGER);
    cls.createIndex(INDEX, OClass.INDEX_TYPE.UNIQUE, PROPERTY_NAME);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.getMetadata().getSchema().getClass(CLASS_NAME).truncate();

    super.afterMethod();
  }

  @Test
  public void testPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 3).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(3)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(3)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test
  public void testRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();

    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  @Test
  public void testRemoveAndPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();
    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    database.commit();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.begin();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1);
    document.save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.rollback();
  }

  @Test
  public void testMultiPut() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    document.field(PROPERTY_NAME, 0);
    document.field(PROPERTY_NAME, 1);
    document.save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    database.commit();

    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  @Test
  public void testPutAfterTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
    database.commit();

    new ODocument(CLASS_NAME).field(PROPERTY_NAME, 2).save();

    try (Stream<ORID> stream = index.getInternal().getRids(2)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  @Test
  public void testRemoveOneWithinTransaction() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();
    document.delete();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    database.commit();

    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test
  public void testPutAfterRemove() {
    if (database.getStorage().isRemote()) {
      throw new SkipException("Test is enabled only for embedded database");
    }

    database.begin();

    final OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX);

    ODocument document = new ODocument(CLASS_NAME).field(PROPERTY_NAME, 1).save();

    document.removeField(PROPERTY_NAME);
    document.save();

    document.field(PROPERTY_NAME, 1).save();

    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX));
    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    database.commit();

    try (Stream<ORID> stream = index.getInternal().getRids(1)) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testInsertionDeletionInsideTx() {
    final String className = "_" + IndexTxAwareOneValueGetTest.class.getSimpleName();
    database.command("create class " + className + " extends V").close();
    database.command("create property " + className + ".name STRING").close();
    database.command("CREATE INDEX " + className + ".name UNIQUE").close();

    database
        .execute(
            "SQL",
            "begin;\n"
                + "insert into "
                + className
                + "(name) values ('c');\n"
                + "let top = (select from "
                + className
                + " where name='c');\n"
                + "delete vertex $top;\n"
                + "commit;\n"
                + "return $top")
        .close();

    try (final OResultSet resultSet = database.query("select * from " + className)) {
      try (Stream<OResult> stream = resultSet.stream()) {
        Assert.assertEquals(stream.count(), 0);
      }
    }
  }
}
