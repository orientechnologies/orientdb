package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Created by tglman on 01/12/15.
 */
public class AlterSuperclassTest {

  private ODatabaseDocumentTx db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + AlterSuperclassTest.class.getSimpleName());
    db.create();
  }

  @AfterMethod
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

  @Test(expectedExceptions = OSchemaException.class)
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

  @Test(expectedExceptions = OSchemaException.class)
  public void testHasAlreadySuperclass() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ParentClass");
    OClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Arrays.asList(classA));
    classChild.addSuperClass(classA);
  }

  /**
   * This tests fixes a problem created in 2.1.9. Issue #5591.
   */
  @Test
  public void testBrokenDbWithMultipleSameSuperClass() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ParentClass");
    OClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Arrays.asList(classA));

    OSchemaShared schemaShared = db.getStorage().getResource(OSchema.class.getSimpleName(), null);

    final ODocument doc = schemaShared.toStream();
    final Collection<ODocument> classes = doc.field("classes");

    for (ODocument d : classes) {
      if ("ChildClass1".equals(d.field("name"))) {
        List<String> superClasses = d.field("superClasses");
        assertTrue(superClasses.contains("ParentClass"));

        superClasses.add("ParentClass");
      }
    }

    schemaShared.fromStream(doc);
  }

  @Test(expectedExceptions = OSchemaException.class)
  public void testSetDuplicateSuperclasses() {
    OSchema schema = db.getMetadata().getSchema();
    OClass classA = schema.createClass("ParentClass");
    OClass classChild = schema.createClass("ChildClass1", classA);
    assertEquals(classChild.getSuperClasses(), Arrays.asList(classA));
    classChild.setSuperClasses(Arrays.asList(classA, classA));
  }

  /**
   * This tests fixes a problem created in Issue #5586.
   * It should not throw ArrayIndexOutOfBoundsException
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
