package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 01/12/15. */
public class AlterSuperclassTest {

  private ODatabaseDocumentTx db;

  @Before
  public void before() {
    //noinspection deprecation
    db = new ODatabaseDocumentTx("memory:" + AlterSuperclassTest.class.getSimpleName());
    db.create();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testSamePropertyCheck() {

    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ParentClass");
    classA.setAbstract(true);
    OProperty property = classA.createProperty("RevNumberNine", OType.INTEGER);
    OClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Arrays.asList(classA));
    OClass classChild2 = schema.createClass("ChildClass2", classChild);
    assertEquals(classChild2.getSuperClasses(), Arrays.asList(classChild));
    classChild2.setSuperClasses(Arrays.asList(classA));
    assertEquals(classChild2.getSuperClasses(), Arrays.asList(classA));
  }

  @Test(expected = OSchemaException.class)
  public void testPropertyNameConflict() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ParentClass");
    classA.setAbstract(true);
    OProperty property = classA.createProperty("RevNumberNine", OType.INTEGER);
    OClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Arrays.asList(classA));
    OClass classChild2 = schema.createClass("ChildClass2");
    classChild2.createProperty("RevNumberNine", OType.STRING);
    classChild2.setSuperClasses(Arrays.asList(classChild));
  }

  @Test(expected = OSchemaException.class)
  public void testHasAlreadySuperclass() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ParentClass");
    OClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Arrays.asList(classA));
    classChild.addSuperClass(classA);
  }

  @Test(expected = OSchemaException.class)
  public void testSetDuplicateSuperclasses() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ParentClass");
    OClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Arrays.asList(classA));
    classChild.setSuperClasses(Arrays.asList(classA, classA));
  }

  /**
   * This tests fixes a problem created in Issue #5586. It should not throw
   * ArrayIndexOutOfBoundsException
   */
  @Test
  public void testBrokenDbAlteringSuperClass() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("BaseClass");
    OClass classChild = schema.createClass("ChildClass1", classA);
    OClass classChild2 = schema.createClass("ChildClass2", classA);

    classChild2.setSuperClass(classChild);

    schema.dropClass("ChildClass2");
  }
}
