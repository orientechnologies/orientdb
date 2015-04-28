package com.orientechnologies.orient.core.metadata;

import java.util.List;

import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class ClassTest {
  private static ODatabaseDocumentTx db                   = null;
  public static final String         SHORTNAME_CLASS_NAME = "TestShortName";

  @BeforeMethod
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("memory:" + ClassTest.class.getSimpleName());
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
    db.create();
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (db.isClosed())
      db.open("admin", "admin");

    db.drop();
  }

  @Test
  public void testShortName() {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass(SHORTNAME_CLASS_NAME);
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());

    final OStorage storage = db.getStorage();

    if (storage instanceof OAbstractPaginatedStorage) {
      final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) storage;
      final OWriteCache writeCache = paginatedStorage.getWriteCache();
      Assert.assertTrue(writeCache.exists(SHORTNAME_CLASS_NAME.toLowerCase() + OPaginatedCluster.DEF_EXTENSION));
    }

    String shortName = "shortname";
    oClass.setShortName(shortName);
    Assert.assertEquals(shortName, oClass.getShortName());
    Assert.assertEquals(shortName, queryShortName());

    // FAILS, saves null value and stores "null" string (not null value) internally
    shortName = "null";
    oClass.setShortName(shortName);
    Assert.assertEquals(shortName, oClass.getShortName());
    Assert.assertEquals(shortName, queryShortName());

    oClass.setShortName(null);
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());

    oClass.setShortName("");
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());

  }

  @Test
  public void testShortNameSnapshot() {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass(SHORTNAME_CLASS_NAME);
    Assert.assertNull(oClass.getShortName());

    String shortName = "shortName";
    oClass.setShortName(shortName);
    Assert.assertEquals(shortName, oClass.getShortName());
    OClass shorted = schema.getClass(shortName);
    Assert.assertNotNull(shorted);
    Assert.assertEquals(shortName, shorted.getShortName());
    OMetadataInternal intern = db.getMetadata();
    OImmutableSchema immSchema = intern.getImmutableSchemaSnapshot();
    shorted = immSchema.getClass(shortName);
    Assert.assertNotNull(shorted);
    Assert.assertEquals(shortName, shorted.getShortName());

  }

  @Test
  public void testRename() {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("ClassName");

    final OStorage storage = db.getStorage();
    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) storage;
    final OWriteCache writeCache = paginatedStorage.getWriteCache();
    Assert.assertTrue(writeCache.exists("classname" + OPaginatedCluster.DEF_EXTENSION));

    oClass.setName("ClassNameNew");

    Assert.assertTrue(!writeCache.exists("classname" + OPaginatedCluster.DEF_EXTENSION));
    Assert.assertTrue(writeCache.exists("classnamenew" + OPaginatedCluster.DEF_EXTENSION));

    oClass.setName("ClassName");

    Assert.assertTrue(!writeCache.exists("classnamenew" + OPaginatedCluster.DEF_EXTENSION));
    Assert.assertTrue(writeCache.exists("classname" + OPaginatedCluster.DEF_EXTENSION));
  }

  @Test
  public void testRenameClusterAlreadyExists() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classOne = schema.createClass("ClassOne");
    OClass classTwo = schema.createClass("ClassTwo");

    final int clusterId = db.addCluster("classthree");
    classTwo.addClusterId(clusterId);

    ODocument document = new ODocument("ClassTwo");
    document.save("classthree");

    document = new ODocument("ClassTwo");
    document.save();

    document = new ODocument("ClassOne");
    document.save();

    Assert.assertEquals(db.countClass("ClassTwo"), 2);
    Assert.assertEquals(db.countClass("ClassOne"), 1);

    classOne.setName("ClassThree");

    final OStorage storage = db.getStorage();
    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) storage;
    final OWriteCache writeCache = paginatedStorage.getWriteCache();

    Assert.assertTrue(writeCache.exists("classone" + OPaginatedCluster.DEF_EXTENSION));

    Assert.assertEquals(db.countClass("ClassTwo"), 2);
    Assert.assertEquals(db.countClass("ClassThree"), 1);

    classOne.setName("ClassOne");
    Assert.assertTrue(writeCache.exists("classone" + OPaginatedCluster.DEF_EXTENSION));

    Assert.assertEquals(db.countClass("ClassTwo"), 2);
    Assert.assertEquals(db.countClass("ClassOne"), 1);
  }

  private String queryShortName() {
    String selectShortNameSQL = "select shortName from ( select flatten(classes) from cluster:internal )" + " where name = \""
        + SHORTNAME_CLASS_NAME + "\"";
    List<ODocument> result = db.command(new OCommandSQL(selectShortNameSQL)).execute();
    Assert.assertEquals(1, result.size());
    return result.get(0).field("shortName");
  }
}
