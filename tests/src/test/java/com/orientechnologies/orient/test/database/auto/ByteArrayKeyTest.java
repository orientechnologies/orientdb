package com.orientechnologies.orient.test.database.auto;

import java.util.Arrays;
import java.util.Collection;

import com.orientechnologies.orient.core.index.OIndexFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * @author Andrey Lomakin
 * @since 03.07.12
 */

@Test
public class ByteArrayKeyTest extends DocumentDBBaseTest {
  private OIndex<?> manualIndex;

  @Parameters(value = "url")
  public ByteArrayKeyTest(@Optional String url) {
    super(url);
  }

  protected OIndex<?> getManualIndex() {
    return database.getMetadata().getIndexManager().getIndex("byte-array-manualIndex");
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OClass byteArrayKeyTest = database.getMetadata().getSchema().createClass("ByteArrayKeyTest");
    byteArrayKeyTest.createProperty("byteArrayKey", OType.BINARY);

    byteArrayKeyTest.createIndex("byteArrayKeyIndex", OClass.INDEX_TYPE.UNIQUE, "byteArrayKey");

    final OClass compositeByteArrayKeyTest = database.getMetadata().getSchema().createClass("CompositeByteArrayKeyTest");
    compositeByteArrayKeyTest.createProperty("byteArrayKey", OType.BINARY);
    compositeByteArrayKeyTest.createProperty("intKey", OType.INTEGER);

    compositeByteArrayKeyTest.createIndex("compositeByteArrayKey", OClass.INDEX_TYPE.UNIQUE, "byteArrayKey", "intKey");

    database
        .getMetadata()
        .getIndexManager()
        .createIndex("byte-array-manualIndex-notunique", "NOTUNIQUE", new OSimpleKeyIndexDefinition(-1, OType.BINARY), null, null,
            null);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    OIndex<?> index = getManualIndex();

    if (index == null) {
      index = database.getMetadata().getIndexManager()
          .createIndex("byte-array-manualIndex", "UNIQUE", new OSimpleKeyIndexDefinition(-1, OType.BINARY), null, null, null);
      this.manualIndex = index;
    } else {
      index = database.getMetadata().getIndexManager().getIndex("byte-array-manualIndex");
      this.manualIndex = index;
    }
  }

  public void testUsage() {
    OIndex<?> index = getManualIndex();
    byte[] key1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1 };
    ODocument doc1 = new ODocument().field("k", "key1");
    index.put(key1, doc1);

    byte[] key2 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 2 };
    ODocument doc2 = new ODocument().field("k", "key1");
    index.put(key2, doc2);

    Assert.assertEquals(index.get(key1), doc1);
    Assert.assertEquals(index.get(key2), doc2);
  }

  public void testAutomaticUsage() {
    byte[] key1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1 };

    ODocument doc1 = new ODocument("ByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.save();

    byte[] key2 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 2 };
    ODocument doc2 = new ODocument("ByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.save();

    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("byteArrayKeyIndex");
    Assert.assertEquals(index.get(key1), doc1);
    Assert.assertEquals(index.get(key2), doc2);
  }

  public void testAutomaticCompositeUsage() {
    byte[] key1 = new byte[] { 1, 2, 3 };
    byte[] key2 = new byte[] { 4, 5, 6 };

    ODocument doc1 = new ODocument("CompositeByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.field("intKey", 1);
    doc1.save();

    ODocument doc2 = new ODocument("CompositeByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.field("intKey", 2);
    doc2.save();

    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("compositeByteArrayKey");
    Assert.assertEquals(index.get(new OCompositeKey(key1, 1)), doc1);
    Assert.assertEquals(index.get(new OCompositeKey(key2, 2)), doc2);
  }

  public void testAutomaticCompositeUsageInTX() {
    byte[] key1 = new byte[] { 7, 8, 9 };
    byte[] key2 = new byte[] { 10, 11, 12 };

    database.begin();
    ODocument doc1 = new ODocument("CompositeByteArrayKeyTest");
    doc1.field("byteArrayKey", key1);
    doc1.field("intKey", 1);
    doc1.save();

    ODocument doc2 = new ODocument("CompositeByteArrayKeyTest");
    doc2.field("byteArrayKey", key2);
    doc2.field("intKey", 2);
    doc2.save();
    database.commit();

    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("compositeByteArrayKey");
    Assert.assertEquals(index.get(new OCompositeKey(key1, 1)), doc1);
    Assert.assertEquals(index.get(new OCompositeKey(key2, 2)), doc2);
  }

  @Test(dependsOnMethods = { "testUsage" })
  public void testRemove() {
    byte[] key1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1 };
    byte[] key2 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 2 };

    OIndex<?> index = getManualIndex();
    Assert.assertTrue(index.remove(key1));

    Assert.assertNull(index.get(key1));
    Assert.assertNotNull(index.get(key2));
  }

  public void testRemoveKeyValue() {
    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("byte-array-manualIndex-notunique");

    byte[] key1 = new byte[] { 0, 1, 2, 3 };
    byte[] key2 = new byte[] { 4, 5, 6, 7 };

    final ODocument doc1 = new ODocument().field("k", "key1");
    final ODocument doc2 = new ODocument().field("k", "key1");
    final ODocument doc3 = new ODocument().field("k", "key2");
    final ODocument doc4 = new ODocument().field("k", "key2");

    doc1.save();
    doc2.save();
    doc3.save();
    doc4.save();

    index.put(key1, doc1);
    index.put(key1, doc2);
    index.put(key2, doc3);
    index.put(key2, doc4);

    Assert.assertTrue(index.remove(key1, doc2));

    Assert.assertEquals(((Collection<?>) index.get(key1)).size(), 1);
    Assert.assertEquals(((Collection<?>) index.get(key2)).size(), 2);
  }

  @Test(dependsOnMethods = { "testAutomaticUsage", "testRemoveKeyValue" })
  public void testContains() {
    byte[] key1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1 };
    byte[] key2 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 2 };

    OIndex<?> autoIndex = database.getMetadata().getIndexManager().getIndex("byteArrayKeyIndex");
    Assert.assertTrue(autoIndex.contains(key1));
    Assert.assertTrue(autoIndex.contains(key2));

    byte[] key3 = new byte[] { 0, 1, 2, 3 };
    byte[] key4 = new byte[] { 4, 5, 6, 7 };

    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("byte-array-manualIndex-notunique");

    Assert.assertTrue(index.contains(key3));
    Assert.assertTrue(index.contains(key4));
  }

  public void testTransactionalUsageWorks() {
    if (database.getURL().startsWith("remote:"))
      return;

    database.begin(OTransaction.TXTYPE.OPTIMISTIC);
    byte[] key3 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 3 };
    ODocument doc1 = new ODocument().field("k", "key3");
    manualIndex.put(key3, doc1);

    byte[] key4 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 4 };
    ODocument doc2 = new ODocument().field("k", "key4");
    manualIndex.put(key4, doc2);

    database.commit();

    Assert.assertEquals(manualIndex.get(key3), doc1);
    Assert.assertEquals(manualIndex.get(key4), doc2);
  }

  @Test(dependsOnMethods = { "testTransactionalUsageWorks" })
  public void testTransactionalUsageBreaks1() {
    if (database.getURL().startsWith("remote:"))
      return;

    database.begin(OTransaction.TXTYPE.OPTIMISTIC);
    OIndex<?> index = getManualIndex();
    byte[] key5 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 5 };
    ODocument doc1 = new ODocument().field("k", "key5");
    index.put(key5, doc1);

    byte[] key6 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 6 };
    ODocument doc2 = new ODocument().field("k", "key6");
    index.put(key6, doc2);

    database.commit();

    Assert.assertEquals(index.get(key5), doc1);
    Assert.assertEquals(index.get(key6), doc2);
  }

  @Test(dependsOnMethods = { "testTransactionalUsageWorks" })
  public void testTransactionalUsageBreaks2() {
    if (database.getURL().startsWith("remote:"))
      return;

    OIndex<?> index = getManualIndex();
    database.begin(OTransaction.TXTYPE.OPTIMISTIC);
    byte[] key7 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 7 };
    ODocument doc1 = new ODocument().field("k", "key7");
    index.put(key7, doc1);

    byte[] key8 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 8 };
    ODocument doc2 = new ODocument().field("k", "key8");
    index.put(key8, doc2);

    database.commit();

    Assert.assertEquals(index.get(key7), doc1);
    Assert.assertEquals(index.get(key8), doc2);
  }
}
