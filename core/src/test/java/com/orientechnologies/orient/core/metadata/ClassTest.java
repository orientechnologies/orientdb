package com.orientechnologies.orient.core.metadata;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.List;
import java.util.Locale;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClassTest {
  public static final String SHORTNAME_CLASS_NAME = "TestShortName";
  private static ODatabaseDocumentTx db = null;

  @Before
  public void setUp() throws Exception {
    db = new ODatabaseDocumentTx("memory:" + ClassTest.class.getSimpleName());
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
    db.create();
  }

  @After
  public void tearDown() throws Exception {
    if (db.isClosed()) db.open("admin", "admin");

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
      Assert.assertTrue(
          writeCache.exists(
              SHORTNAME_CLASS_NAME.toLowerCase(Locale.ENGLISH) + OPaginatedCluster.DEF_EXTENSION));
    }

    String shortName = "shortname";
    oClass.setShortName(shortName);
    assertEquals(shortName, oClass.getShortName());
    assertEquals(shortName, queryShortName());

    // FAILS, saves null value and stores "null" string (not null value) internally
    shortName = "null";
    oClass.setShortName(shortName);
    assertEquals(shortName, oClass.getShortName());
    assertEquals(shortName, queryShortName());

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
    assertEquals(shortName, oClass.getShortName());
    OClass shorted = schema.getClass(shortName);
    Assert.assertNotNull(shorted);
    assertEquals(shortName, shorted.getShortName());
    OMetadataInternal intern = db.getMetadata();
    OImmutableSchema immSchema = intern.getImmutableSchemaSnapshot();
    shorted = immSchema.getClass(shortName);
    Assert.assertNotNull(shorted);
    assertEquals(shortName, shorted.getShortName());
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

    assertEquals(db.countClass("ClassTwo"), 2);
    assertEquals(db.countClass("ClassOne"), 1);

    classOne.setName("ClassThree");

    final OStorage storage = db.getStorage();
    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) storage;
    final OWriteCache writeCache = paginatedStorage.getWriteCache();

    Assert.assertTrue(writeCache.exists("classone" + OPaginatedCluster.DEF_EXTENSION));

    assertEquals(db.countClass("ClassTwo"), 2);
    assertEquals(db.countClass("ClassThree"), 1);

    classOne.setName("ClassOne");
    Assert.assertTrue(writeCache.exists("classone" + OPaginatedCluster.DEF_EXTENSION));

    assertEquals(db.countClass("ClassTwo"), 2);
    assertEquals(db.countClass("ClassOne"), 1);
  }

  @Test
  public void testOClassAndOPropertyDescription() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("DescriptionTest");
    OProperty oProperty = oClass.createProperty("property", OType.STRING);
    oClass.setDescription("DescriptionTest-class-description");
    oProperty.setDescription("DescriptionTest-property-description");
    assertEquals(oClass.getDescription(), "DescriptionTest-class-description");
    assertEquals(oProperty.getDescription(), "DescriptionTest-property-description");
    oSchema.reload();
    oClass = oSchema.getClass("DescriptionTest");
    oProperty = oClass.getProperty("property");
    assertEquals(oClass.getDescription(), "DescriptionTest-class-description");
    assertEquals(oProperty.getDescription(), "DescriptionTest-property-description");

    oClass = db.getMetadata().getImmutableSchemaSnapshot().getClass("DescriptionTest");
    oProperty = oClass.getProperty("property");
    assertEquals(oClass.getDescription(), "DescriptionTest-class-description");
    assertEquals(oProperty.getDescription(), "DescriptionTest-property-description");
  }

  private String queryShortName() {
    String selectShortNameSQL =
        "select shortName from ( select flatten(classes) from cluster:internal )"
            + " where name = \""
            + SHORTNAME_CLASS_NAME
            + "\"";
    List<ODocument> result = db.command(new OCommandSQL(selectShortNameSQL)).execute();
    assertEquals(1, result.size());
    return result.get(0).field("shortName");
  }
}
