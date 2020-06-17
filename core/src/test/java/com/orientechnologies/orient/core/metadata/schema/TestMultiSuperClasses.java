package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMultiSuperClasses {
  private ODatabaseDocumentTx db;

  @Before
  public void setUp() {
    db = new ODatabaseDocumentTx("memory:" + TestMultiSuperClasses.class.getSimpleName());
    if (db.exists()) {
      db.open("admin", "admin");
    } else db.create();
  }

  @After
  public void after() {
    db.close();
  }

  @Test
  public void testClassCreation() {
    OSchema oSchema = db.getMetadata().getSchema();

    OClass aClass = oSchema.createAbstractClass("javaA");
    OClass bClass = oSchema.createAbstractClass("javaB");
    aClass.createProperty("propertyInt", OType.INTEGER);
    bClass.createProperty("propertyDouble", OType.DOUBLE);
    OClass cClass = oSchema.createClass("javaC", aClass, bClass);
    testClassCreationBranch(aClass, bClass, cClass);
    oSchema.reload();
    testClassCreationBranch(aClass, bClass, cClass);
    oSchema = db.getMetadata().getImmutableSchemaSnapshot();
    aClass = oSchema.getClass("javaA");
    bClass = oSchema.getClass("javaB");
    cClass = oSchema.getClass("javaC");
    testClassCreationBranch(aClass, bClass, cClass);
  }

  private void testClassCreationBranch(OClass aClass, OClass bClass, OClass cClass) {
    assertNotNull(aClass.getSuperClasses());
    assertEquals(aClass.getSuperClasses().size(), 0);
    assertNotNull(bClass.getSuperClassesNames());
    assertEquals(bClass.getSuperClassesNames().size(), 0);
    assertNotNull(cClass.getSuperClassesNames());
    assertEquals(cClass.getSuperClassesNames().size(), 2);

    List<? extends OClass> superClasses = cClass.getSuperClasses();
    assertTrue(superClasses.contains(aClass));
    assertTrue(superClasses.contains(bClass));
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
    assertTrue(aClass.isSuperClassOf(cClass));
    assertTrue(bClass.isSuperClassOf(cClass));

    OProperty property = cClass.getProperty("propertyInt");
    assertEquals(OType.INTEGER, property.getType());
    property = cClass.propertiesMap().get("propertyInt");
    assertEquals(OType.INTEGER, property.getType());

    property = cClass.getProperty("propertyDouble");
    assertEquals(OType.DOUBLE, property.getType());
    property = cClass.propertiesMap().get("propertyDouble");
    assertEquals(OType.DOUBLE, property.getType());
  }

  @Test
  public void testSql() {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass aClass = oSchema.createAbstractClass("sqlA");
    OClass bClass = oSchema.createAbstractClass("sqlB");
    OClass cClass = oSchema.createClass("sqlC");
    db.command(new OCommandSQL("alter class sqlC superclasses sqlA, sqlB")).execute();
    oSchema.reload();
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
    db.command(new OCommandSQL("alter class sqlC superclass sqlA")).execute();
    oSchema.reload();
    assertTrue(cClass.isSubClassOf(aClass));
    assertFalse(cClass.isSubClassOf(bClass));
    db.command(new OCommandSQL("alter class sqlC superclass +sqlB")).execute();
    oSchema.reload();
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
    db.command(new OCommandSQL("alter class sqlC superclass -sqlA")).execute();
    oSchema.reload();
    assertFalse(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
  }

  @Test
  public void testCreationBySql() {
    final OSchema oSchema = db.getMetadata().getSchema();

    db.command(new OCommandSQL("create class sql2A abstract")).execute();
    db.command(new OCommandSQL("create class sql2B abstract")).execute();
    db.command(new OCommandSQL("create class sql2C extends sql2A, sql2B abstract")).execute();
    oSchema.reload();
    OClass aClass = oSchema.getClass("sql2A");
    OClass bClass = oSchema.getClass("sql2B");
    OClass cClass = oSchema.getClass("sql2C");
    assertNotNull(aClass);
    assertNotNull(bClass);
    assertNotNull(cClass);
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
  }

  @Test(
      expected = OSchemaException.class) // , expectedExceptionsMessageRegExp = "(?s).*recursion.*"
  // )
  public void testPreventionOfCycles() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass aClass = oSchema.createAbstractClass("cycleA");
    OClass bClass = oSchema.createAbstractClass("cycleB", aClass);
    OClass cClass = oSchema.createAbstractClass("cycleC", bClass);

    aClass.setSuperClasses(Arrays.asList(cClass));
  }

  @Test
  public void testParametersImpactGoodScenario() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass aClass = oSchema.createAbstractClass("impactGoodA");
    aClass.createProperty("property", OType.STRING);
    OClass bClass = oSchema.createAbstractClass("impactGoodB");
    bClass.createProperty("property", OType.STRING);
    OClass cClass = oSchema.createAbstractClass("impactGoodC", aClass, bClass);
    assertTrue(cClass.existsProperty("property"));
  }

  @Test(
      expected = OSchemaException.class) // }, expectedExceptionsMessageRegExp = "(?s).*conflict.*")
  public void testParametersImpactBadScenario() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass aClass = oSchema.createAbstractClass("impactBadA");
    aClass.createProperty("property", OType.STRING);
    OClass bClass = oSchema.createAbstractClass("impactBadB");
    bClass.createProperty("property", OType.INTEGER);
    oSchema.createAbstractClass("impactBadC", aClass, bClass);
  }

  @Test
  public void testCreationOfClassWithV() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oRestrictedClass = oSchema.getClass("ORestricted");
    OClass vClass = oSchema.getClass("V");
    vClass.setSuperClasses(Arrays.asList(oRestrictedClass));
    OClass dummy1Class = oSchema.createClass("Dummy1", oRestrictedClass, vClass);
    OClass dummy2Class = oSchema.createClass("Dummy2");
    OClass dummy3Class = oSchema.createClass("Dummy3", dummy1Class, dummy2Class);
    assertNotNull(dummy3Class);
  }
}
