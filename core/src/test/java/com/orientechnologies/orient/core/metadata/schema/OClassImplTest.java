package com.orientechnologies.orient.core.metadata.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OClassImplTest {

  private ODatabaseDocumentTx db;

  @BeforeMethod
  public void setUp() {
    db = new ODatabaseDocumentTx("memory:" + OClassImplTest.class.getSimpleName());
    if (db.exists()) {
      db.open("admin", "admin");
    } else
      db.create();
  }

  @AfterMethod
  public void after() {
    db.close();
  }

  @AfterClass
  public void afterClass() {
    db.open("admin", "admin");
    db.drop();
  }

  /**
   * If class was not abstract and we call {@code setAbstract(false)} clusters should not be changed.
   * 
   * @throws Exception
   */
  @Test
  public void testSetAbstractClusterNotChanged() throws Exception {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createClass("Test1");
    final int oldClusterId = oClass.getDefaultClusterId();

    oClass.setAbstract(false);

    Assert.assertEquals(oClass.getDefaultClusterId(), oldClusterId);
  }

  /**
   * If class was abstract and we call {@code setAbstract(false)} a new non default cluster should be created.
   * 
   * @throws Exception
   */
  @Test
  public void testSetAbstractShouldCreateNewClusters() throws Exception {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createAbstractClass("Test2");

    oClass.setAbstract(false);

    Assert.assertFalse(oClass.getDefaultClusterId() == -1);
    Assert.assertFalse(oClass.getDefaultClusterId() == db.getDefaultClusterId());
  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testCreatePropertyFailOnExistingData() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test3");

    ODocument document = new ODocument("Test3");

    document.field("some", "String");
    db.save(document);
    db.commit();
    oClass.createProperty("some", OType.INTEGER);

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkList() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test4");

    ODocument document = new ODocument("Test4");
    ArrayList<ODocument> list = new ArrayList<ODocument>();
    list.add(new ODocument("Test4"));
    document.field("some", list);
    db.save(document);
    db.commit();
    oClass.createProperty("some", OType.EMBEDDEDLIST);

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkSet() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test5");

    ODocument document = new ODocument("Test5");
    Set<ODocument> list = new HashSet<ODocument>();
    list.add(new ODocument("Test5"));
    document.field("somelinkset", list);
    db.save(document);
    db.commit();
    oClass.createProperty("somelinkset", OType.EMBEDDEDSET);

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddetSet() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test6");

    ODocument document = new ODocument("Test6");
    Set<ODocument> list = new HashSet<ODocument>();
    list.add(new ODocument("Test6"));
    document.field("someembededset", list, OType.EMBEDDEDSET);
    db.save(document);
    db.commit();
    oClass.createProperty("someembededset", OType.LINKSET);

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddedList() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test7");

    ODocument document = new ODocument("Test7");
    List<ODocument> list = new ArrayList<ODocument>();
    list.add(new ODocument("Test7"));
    document.field("someembeddedlist", list, OType.EMBEDDEDLIST);
    db.save(document);
    db.commit();
    oClass.createProperty("someembeddedlist", OType.LINKLIST);

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddedMap() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test8");

    ODocument document = new ODocument("Test8");
    Map<String, ODocument> map = new HashMap<String, ODocument>();
    map.put("test", new ODocument("Test8"));
    document.field("someembededmap", map, OType.EMBEDDEDMAP);
    db.save(document);
    db.commit();
    oClass.createProperty("someembededmap", OType.LINKMAP);

  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkMap() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test9");

    ODocument document = new ODocument("Test9");
    Map<String, ODocument> map = new HashMap<String, ODocument>();
    map.put("test", new ODocument("Test8"));
    document.field("somelinkmap", map, OType.LINKMAP);
    db.save(document);
    db.commit();
    oClass.createProperty("somelinkmap", OType.EMBEDDEDMAP);

  }

  @Test
  public void testCreatePropertyCastable() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test10");

    ODocument document = new ODocument("Test10");
    // TODO add boolan and byte
    document.field("test1", (short) 1);
    document.field("test2", 1);
    document.field("test3", 4L);
    document.field("test4", 3.0f);
    document.field("test5", 3.0D);
    document.field("test6", 4);
    db.save(document);
    db.commit();
    oClass.createProperty("test1", OType.INTEGER);
    oClass.createProperty("test2", OType.LONG);
    oClass.createProperty("test3", OType.DOUBLE);
    oClass.createProperty("test4", OType.DOUBLE);
    oClass.createProperty("test5", OType.DECIMAL);
    oClass.createProperty("test6", OType.FLOAT);

    ODocument doc1 = db.load(document.getIdentity());
    assertEquals(doc1.fieldType("test1"), OType.INTEGER);
    assertTrue(doc1.field("test1") instanceof Integer);
    assertEquals(doc1.fieldType("test2"), OType.LONG);
    assertTrue(doc1.field("test2") instanceof Long);
    assertEquals(doc1.fieldType("test3"), OType.DOUBLE);
    assertTrue(doc1.field("test3") instanceof Double);
    assertEquals(doc1.fieldType("test4"), OType.DOUBLE);
    assertTrue(doc1.field("test4") instanceof Double);
    assertEquals(doc1.fieldType("test5"), OType.DECIMAL);
    assertTrue(doc1.field("test5") instanceof BigDecimal);
    assertEquals(doc1.fieldType("test6"), OType.FLOAT);
    assertTrue(doc1.field("test6") instanceof Float);
  }

  @Test
  public void testCreatePropertyCastableColection() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test11");

    ODocument document = new ODocument("Test11");
    document.field("test1", new ArrayList<ODocument>(), OType.EMBEDDEDLIST);
    document.field("test2", new ArrayList<ODocument>(), OType.LINKLIST);
    document.field("test3", new HashSet<ODocument>(), OType.EMBEDDEDSET);
    document.field("test4", new HashSet<ODocument>(), OType.LINKSET);
    document.field("test5", new HashMap<String, ODocument>(), OType.EMBEDDEDMAP);
    document.field("test6", new HashMap<String, ODocument>(), OType.LINKMAP);
    db.save(document);
    db.commit();
    oClass.createProperty("test1", OType.LINKLIST);
    oClass.createProperty("test2", OType.EMBEDDEDLIST);
    oClass.createProperty("test3", OType.LINKSET);
    oClass.createProperty("test4", OType.EMBEDDEDSET);
    oClass.createProperty("test5", OType.LINKMAP);
    oClass.createProperty("test6", OType.EMBEDDEDMAP);

    ODocument doc1 = db.load(document.getIdentity());
    assertEquals(doc1.fieldType("test1"), OType.LINKLIST);
    assertEquals(doc1.fieldType("test2"), OType.EMBEDDEDLIST);
    assertEquals(doc1.fieldType("test3"), OType.LINKSET);
    assertEquals(doc1.fieldType("test4"), OType.EMBEDDEDSET);
    assertEquals(doc1.fieldType("test5"), OType.LINKMAP);
    assertEquals(doc1.fieldType("test6"), OType.EMBEDDEDMAP);

  }

}
