package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;

import static org.testng.Assert.*;

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

  @Test
  public void testCreatePropertyIdKeep() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test12");
    OProperty prop = oClass.createProperty("test2", OType.STRING);
    Integer id = prop.getId();
    oClass.dropProperty("test2");
    prop = oClass.createProperty("test2", OType.STRING);
    assertEquals(id, prop.getId());
  }

  @Test
  public void testRenameProperty() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test13");
    OProperty prop = oClass.createProperty("test1", OType.STRING);
    Integer id = prop.getId();
    prop.setName("test2");
    assertNotEquals(id, prop.getId());
  }

  @Test
  public void testChangeTypeProperty() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test14");
    OProperty prop = oClass.createProperty("test1", OType.SHORT);
    Integer id = prop.getId();
    prop.setType(OType.INTEGER);
    assertNotEquals(id, prop.getId());
  }

  @Test
  public void testRenameBackProperty() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test15");
    OProperty prop = oClass.createProperty("test1", OType.STRING);
    Integer id = prop.getId();
    prop.setName("test2");
    assertNotEquals(id, prop.getId());
    prop.setName("test1");
    assertEquals(id, prop.getId());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetUncastableType() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test16");
    OProperty prop = oClass.createProperty("test1", OType.STRING);
    prop.setType(OType.INTEGER);
  }

  @Test
  public void testFindById() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test17");
    OProperty prop = oClass.createProperty("testaaa", OType.STRING);
    OGlobalProperty global = oSchema.getGlobalPropertyById(prop.getId());

    assertEquals(prop.getId(), global.getId());
    assertEquals(prop.getName(), global.getName());
    assertEquals(prop.getType(), global.getType());
  }

  @Test
  public void testFindByIdDrop() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test18");
    OProperty prop = oClass.createProperty("testaaa", OType.STRING);
    Integer id = prop.getId();
    oClass.dropProperty("testaaa");
    OGlobalProperty global = oSchema.getGlobalPropertyById(id);

    assertEquals(id, global.getId());
    assertEquals("testaaa", global.getName());
    assertEquals(OType.STRING, global.getType());
  }

  @Test
  public void testChangePropertyTypeCastable() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test19");

    oClass.createProperty("test1", OType.SHORT);
    oClass.createProperty("test2", OType.INTEGER);
    oClass.createProperty("test3", OType.LONG);
    oClass.createProperty("test4", OType.FLOAT);
    oClass.createProperty("test5", OType.DOUBLE);
    oClass.createProperty("test6", OType.INTEGER);

    ODocument document = new ODocument("Test19");
    // TODO add boolan and byte
    document.field("test1", (short) 1);
    document.field("test2", 1);
    document.field("test3", 4L);
    document.field("test4", 3.0f);
    document.field("test5", 3.0D);
    document.field("test6", 4);
    db.save(document);
    db.commit();

    oClass.getProperty("test1").setType(OType.INTEGER);
    oClass.getProperty("test2").setType(OType.LONG);
    oClass.getProperty("test3").setType(OType.DOUBLE);
    oClass.getProperty("test4").setType(OType.DOUBLE);
    oClass.getProperty("test5").setType(OType.DECIMAL);
    oClass.getProperty("test6").setType(OType.FLOAT);

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
  public void testChangePropertyName() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test20");

    oClass.createProperty("test1", OType.SHORT);
    oClass.createProperty("test2", OType.INTEGER);
    oClass.createProperty("test3", OType.LONG);
    oClass.createProperty("test4", OType.FLOAT);
    oClass.createProperty("test5", OType.DOUBLE);
    oClass.createProperty("test6", OType.INTEGER);

    ODocument document = new ODocument("Test20");
    // TODO add boolan and byte
    document.field("test1", (short) 1);
    document.field("test2", 1);
    document.field("test3", 4L);
    document.field("test4", 3.0f);
    document.field("test5", 3.0D);
    document.field("test6", 4);
    db.save(document);
    db.commit();

    oClass.getProperty("test1").setName("test1a");
    oClass.getProperty("test2").setName("test2a");
    oClass.getProperty("test3").setName("test3a");
    oClass.getProperty("test4").setName("test4a");
    oClass.getProperty("test5").setName("test5a");
    oClass.getProperty("test6").setName("test6a");

    ODocument doc1 = db.load(document.getIdentity());
    assertEquals(doc1.fieldType("test1a"), OType.SHORT);
    assertTrue(doc1.field("test1a") instanceof Short);
    assertEquals(doc1.fieldType("test2a"), OType.INTEGER);
    assertTrue(doc1.field("test2a") instanceof Integer);
    assertEquals(doc1.fieldType("test3a"), OType.LONG);
    assertTrue(doc1.field("test3a") instanceof Long);
    assertEquals(doc1.fieldType("test4a"), OType.FLOAT);
    assertTrue(doc1.field("test4a") instanceof Float);
    assertEquals(doc1.fieldType("test5a"), OType.DOUBLE);
    assertTrue(doc1.field("test5") instanceof Double);
    assertEquals(doc1.fieldType("test6a"), OType.INTEGER);
    assertTrue(doc1.field("test6a") instanceof Integer);
  }

  @Test
  public void testCreatePropertyCastableColectionNoCache() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test11bis");

    final ODocument document = new ODocument("Test11bis");
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

    ExecutorService executor = Executors.newSingleThreadExecutor();

    Future<ODocument> future = executor.submit(new Callable<ODocument>() {
      @Override
      public ODocument call() throws Exception {
        ODocument doc1 = db.copy().load(document.getIdentity());
        assertEquals(doc1.fieldType("test1"), OType.LINKLIST);
        assertEquals(doc1.fieldType("test2"), OType.EMBEDDEDLIST);
        assertEquals(doc1.fieldType("test3"), OType.LINKSET);
        assertEquals(doc1.fieldType("test4"), OType.EMBEDDEDSET);
        assertEquals(doc1.fieldType("test5"), OType.LINKMAP);
        assertEquals(doc1.fieldType("test6"), OType.EMBEDDEDMAP);
        return doc1;
      }
    });

    try {
      future.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AssertionError) {
        throw (AssertionError) e.getCause();
      }
    }

    executor.shutdown();

  }

  @Test
  public void testClassNameSyntax() {

    final OSchema oSchema = db.getMetadata().getSchema();
    assertNotNull(oSchema.createClass("OClassImplTesttestClassNameSyntax"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestClassNameSyntax"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestClassNameSyntax_"));
    assertNotNull(oSchema.createClass("_OClassImplTestte_stClassNameSyntax_"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestClassNameSyntax_12"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestCla23ssNameSyntax_12"));
    assertNotNull(oSchema.createClass("$OClassImplTesttestCla23ssNameSyntax_12"));
    assertNotNull(oSchema.createClass("OClassImplTesttestC$la23ssNameSyntax_12"));
    assertNotNull(oSchema.createClass("oOClassImplTesttestC$la23ssNameSyntax_12"));
    String[] invalidClassNames = { "foo bar", "12", "#12", "12AAA", ",asdfasdf", "adsf,asdf", "asdf.sadf", ".asdf", "asdfaf.", "asdf:asdf" };
    for (String s : invalidClassNames) {
      try {
        oSchema.createClass(s);
        fail("class with invalid name is incorrectly created: '" + s + "'");
      } catch (Exception e) {

      }
    }

  }
}
