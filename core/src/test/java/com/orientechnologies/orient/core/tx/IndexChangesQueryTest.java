package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 28/05/17. */
public class IndexChangesQueryTest {
  public static final String CLASS_NAME = "idxTxAwareMultiValueGetEntriesTest";
  private static final String FIELD_NAME = "value";
  private static final String INDEX_NAME = "idxTxAwareMultiValueGetEntriesTestIndex";
  private OrientDB orientDB;
  private ODatabaseDocumentInternal database;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    database =
        (ODatabaseDocumentInternal)
            orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    final OSchema schema = database.getMetadata().getSchema();
    final OClass cls = schema.createClass(CLASS_NAME);
    cls.createProperty(FIELD_NAME, OType.INTEGER);
    cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.NOTUNIQUE, FIELD_NAME);
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
  }

  @Test
  public void testMultiplePut() {
    database.begin();

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    ODocument doc = new ODocument(CLASS_NAME);
    doc.field(FIELD_NAME, 1);
    doc.save();

    ODocument doc1 = new ODocument(CLASS_NAME);
    doc1.field(FIELD_NAME, 2);
    doc1.save();
    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Assert.assertFalse(fetchCollectionFromIndex(index, 1).isEmpty());
    Assert.assertFalse((fetchCollectionFromIndex(index, 2)).isEmpty());

    database.commit();

    Assert.assertEquals(index.getInternal().size(), 2);
    Assert.assertFalse((fetchCollectionFromIndex(index, 1)).isEmpty());
    Assert.assertFalse((fetchCollectionFromIndex(index, 2)).isEmpty());
  }

  private static Collection<ORID> fetchCollectionFromIndex(OIndex index, int key) {
    try (Stream<ORID> stream = index.getInternal().getRids(key)) {
      return stream.collect(Collectors.toList());
    }
  }

  @Test
  public void testClearAndPut() {
    database.begin();

    ODocument doc1 = new ODocument(CLASS_NAME);
    doc1.field(FIELD_NAME, 1);
    doc1.save();

    ODocument doc2 = new ODocument(CLASS_NAME);
    doc2.field(FIELD_NAME, 1);
    doc2.save();

    ODocument doc3 = new ODocument(CLASS_NAME);
    doc3.field(FIELD_NAME, 2);
    doc3.save();

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    database.commit();

    Assert.assertEquals(3, index.getInternal().size());
    Assert.assertEquals(2, (fetchCollectionFromIndex(index, 1)).size());
    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 2)).size());

    database.begin();

    doc1.delete();
    doc2.delete();
    doc3.delete();

    doc3 = new ODocument(CLASS_NAME);
    doc3.field(FIELD_NAME, 1);
    doc3.save();

    ODocument doc = new ODocument(CLASS_NAME);
    doc.field(FIELD_NAME, 2);
    doc.save();

    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 1)).size());
    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 2)).size());

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Assert.assertEquals(3, index.getInternal().size());
    Assert.assertEquals(2, (fetchCollectionFromIndex(index, 1)).size());
    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 2)).size());
  }
}
