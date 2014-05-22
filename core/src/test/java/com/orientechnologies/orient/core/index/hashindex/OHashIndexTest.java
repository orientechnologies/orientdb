package com.orientechnologies.orient.core.index.hashindex;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
@Test
public class OHashIndexTest {
  private ODatabaseDocumentTx db;

  @BeforeClass
  public void setUp() throws Exception {

    db = new ODatabaseDocumentTx("plocal:target/hashIndexTest");

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (!db.isClosed())
      db.close();
  }

  public void testCreateAutomaticHashIndex() throws Exception {
    final OClass oClass = db.getMetadata().getSchema().createClass("testClass");
    oClass.createProperty("name", OType.STRING);
    final OIndex<?> index = oClass.createIndex("testClassNameIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "name");

    Assert.assertNotNull(index);
  }

  public void testCreateManualHashIndex() throws Exception {
    final OIndex<?> index = db
        .getMetadata()
        .getIndexManager()
        .createIndex("manualHashIndex", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString(),
            new OSimpleKeyIndexDefinition(OType.STRING), null, null, null);

    Assert.assertNotNull(index);
  }

  @Test(dependsOnMethods = "testCreateManualHashIndex")
  public void testStoreDataAfterDBWasClosed() {
    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("manualHashIndex");

    for (int i = 0; i < 1000000; i++) {
      index.put(i + "", new ORecordId(0, OClusterPositionFactory.INSTANCE.valueOf(i)));
    }

    for (int i = 0; i < 1000000; i++) {
      if (i % 2 == 0)
        index.remove(i + "");

    }

    db.close();

    db.open("admin", "admin");
    index = db.getMetadata().getIndexManager().getIndex("manualHashIndex");
    for (int i = 1; i < 1000000; i += 2)
      Assert.assertEquals(index.get(i + ""), new ORecordId(0, OClusterPositionFactory.INSTANCE.valueOf(i)));

    for (int i = 0; i < 1000000; i += 2)
      Assert.assertNull(index.get(i + ""));
  }
}
