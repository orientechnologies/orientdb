package com.orientechnologies.orient.core.sql.executor;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreatePropertyStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreatePropertyStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  private static final String PROP_NAME = "name";

  private static final String PROP_DIVISION = "division";

  private static final String PROP_OFFICERS = "officers";

  private static final String PROP_ID = "id";

  @Test
  public void testBasicCreateProperty() throws Exception {
    db.command("CREATE class testBasicCreateProperty").close();
    db.command("CREATE property testBasicCreateProperty.name STRING").close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testBasicCreateProperty");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testBasicCreateProperty.name");
    assertEquals(nameProperty.getType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testBasicUnsafeCreateProperty() throws Exception {
    db.command("CREATE class testBasicUnsafeCreateProperty").close();
    db.command("CREATE property testBasicUnsafeCreateProperty.name STRING UNSAFE").close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testBasicUnsafeCreateProperty");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testBasicUnsafeCreateProperty.name");
    assertEquals(nameProperty.getType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreatePropertyWithLinkedClass() throws Exception {
    db.command("CREATE class testCreatePropertyWithLinkedClass_1").close();
    db.command("CREATE class testCreatePropertyWithLinkedClass_2").close();
    db.command(
            "CREATE property testCreatePropertyWithLinkedClass_2.division LINK testCreatePropertyWithLinkedClass_1")
        .close();

    OClass companyClass =
        db.getMetadata().getSchema().getClass("testCreatePropertyWithLinkedClass_2");
    OProperty nameProperty = companyClass.getProperty(PROP_DIVISION);

    assertEquals(nameProperty.getName(), PROP_DIVISION);
    assertEquals(nameProperty.getFullName(), "testCreatePropertyWithLinkedClass_2.division");
    assertEquals(nameProperty.getType(), OType.LINK);
    assertEquals(nameProperty.getLinkedClass().getName(), "testCreatePropertyWithLinkedClass_1");
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreatePropertyWithEmbeddedType() throws Exception {
    db.command("CREATE Class testCreatePropertyWithEmbeddedType").close();
    db.command("CREATE Property testCreatePropertyWithEmbeddedType.officers EMBEDDEDLIST STRING")
        .close();

    OClass companyClass =
        db.getMetadata().getSchema().getClass("testCreatePropertyWithEmbeddedType");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), "testCreatePropertyWithEmbeddedType.officers");
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateMandatoryProperty() throws Exception {
    db.command("CREATE class testCreateMandatoryProperty").close();
    db.command("CREATE property testCreateMandatoryProperty.name STRING (MANDATORY)").close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testCreateMandatoryProperty");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testCreateMandatoryProperty.name");
    assertTrue(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateNotNullProperty() throws Exception {
    db.command("CREATE class testCreateNotNullProperty").close();
    db.command("CREATE property testCreateNotNullProperty.name STRING (NOTNULL)").close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testCreateNotNullProperty");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testCreateNotNullProperty.name");
    assertFalse(nameProperty.isMandatory());
    assertTrue(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateReadOnlyProperty() throws Exception {
    db.command("CREATE class testCreateReadOnlyProperty").close();
    db.command("CREATE property testCreateReadOnlyProperty.name STRING (READONLY)").close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testCreateReadOnlyProperty");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testCreateReadOnlyProperty.name");
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertTrue(nameProperty.isReadonly());
  }

  @Test
  public void testCreateReadOnlyFalseProperty() throws Exception {
    db.command("CREATE class testCreateReadOnlyFalseProperty").close();
    db.command("CREATE property testCreateReadOnlyFalseProperty.name STRING (READONLY false)")
        .close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testCreateReadOnlyFalseProperty");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testCreateReadOnlyFalseProperty.name");
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateMandatoryPropertyWithEmbeddedType() throws Exception {
    db.command("CREATE Class testCreateMandatoryPropertyWithEmbeddedType").close();
    db.command(
            "CREATE Property testCreateMandatoryPropertyWithEmbeddedType.officers EMBEDDEDLIST STRING (MANDATORY)")
        .close();

    OClass companyClass =
        db.getMetadata().getSchema().getClass("testCreateMandatoryPropertyWithEmbeddedType");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(
        nameProperty.getFullName(), "testCreateMandatoryPropertyWithEmbeddedType.officers");
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertTrue(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateUnsafePropertyWithEmbeddedType() throws Exception {
    db.command("CREATE Class testCreateUnsafePropertyWithEmbeddedType").close();
    db.command(
            "CREATE Property testCreateUnsafePropertyWithEmbeddedType.officers EMBEDDEDLIST STRING UNSAFE")
        .close();

    OClass companyClass =
        db.getMetadata().getSchema().getClass("testCreateUnsafePropertyWithEmbeddedType");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), "testCreateUnsafePropertyWithEmbeddedType.officers");
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
  }

  @Test
  public void testComplexCreateProperty() throws Exception {
    db.command("CREATE Class testComplexCreateProperty").close();
    db.command(
            "CREATE Property testComplexCreateProperty.officers EMBEDDEDLIST STRING (MANDATORY, READONLY, NOTNULL) UNSAFE")
        .close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testComplexCreateProperty");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), "testComplexCreateProperty.officers");
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertTrue(nameProperty.isMandatory());
    assertTrue(nameProperty.isNotNull());
    assertTrue(nameProperty.isReadonly());
  }

  @Test
  public void testLinkedTypeDefaultAndMinMaxUnsafeProperty() throws Exception {
    db.command("CREATE CLASS testLinkedTypeDefaultAndMinMaxUnsafeProperty").close();
    db.command(
            "CREATE PROPERTY testLinkedTypeDefaultAndMinMaxUnsafeProperty.id EMBEDDEDLIST Integer (DEFAULT 5, MIN 1, MAX 10) UNSAFE")
        .close();

    OClass companyClass =
        db.getMetadata().getSchema().getClass("testLinkedTypeDefaultAndMinMaxUnsafeProperty");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), "testLinkedTypeDefaultAndMinMaxUnsafeProperty.id");
    assertEquals(idProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(idProperty.getLinkedType(), OType.INTEGER);
    assertFalse(idProperty.isMandatory());
    assertFalse(idProperty.isNotNull());
    assertFalse(idProperty.isReadonly());
    assertEquals(idProperty.getDefaultValue(), "5");
    assertEquals(idProperty.getMin(), "1");
    assertEquals(idProperty.getMax(), "10");
  }

  @Test
  public void testDefaultAndMinMaxUnsafeProperty() throws Exception {
    db.command("CREATE CLASS testDefaultAndMinMaxUnsafeProperty").close();
    db.command(
            "CREATE PROPERTY testDefaultAndMinMaxUnsafeProperty.id INTEGER (DEFAULT 5, MIN 1, MAX 10) UNSAFE")
        .close();

    OClass companyClass =
        db.getMetadata().getSchema().getClass("testDefaultAndMinMaxUnsafeProperty");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), "testDefaultAndMinMaxUnsafeProperty.id");
    assertEquals(idProperty.getType(), OType.INTEGER);
    assertEquals(idProperty.getLinkedType(), null);
    assertFalse(idProperty.isMandatory());
    assertFalse(idProperty.isNotNull());
    assertFalse(idProperty.isReadonly());
    assertEquals(idProperty.getDefaultValue(), "5");
    assertEquals(idProperty.getMin(), "1");
    assertEquals(idProperty.getMax(), "10");
  }

  @Test
  public void testExtraSpaces() throws Exception {
    db.command("CREATE CLASS testExtraSpaces").close();
    db.command("CREATE PROPERTY testExtraSpaces.id INTEGER  ( DEFAULT  5 ,  MANDATORY  )  UNSAFE ")
        .close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testExtraSpaces");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), "testExtraSpaces.id");
    assertEquals(idProperty.getType(), OType.INTEGER);
    assertEquals(idProperty.getLinkedType(), null);
    assertTrue(idProperty.isMandatory());
    assertEquals(idProperty.getDefaultValue(), "5");
  }

  @Test(expected = OCommandExecutionException.class)
  public void testInvalidAttributeName() throws Exception {
    db.command("CREATE CLASS OCommandExecutionException").close();
    db.command(
            "CREATE PROPERTY OCommandExecutionException.id INTEGER (MANDATORY, INVALID, NOTNULL)  UNSAFE")
        .close();
  }

  @Test(expected = OCommandExecutionException.class)
  public void testMissingAttributeValue() throws Exception {
    db.command("CREATE CLASS testMissingAttributeValue").close();
    db.command("CREATE PROPERTY testMissingAttributeValue.id INTEGER (DEFAULT)  UNSAFE").close();
  }

  @Test
  public void testMandatoryAsLinkedName() throws Exception {
    db.command("CREATE CLASS testMandatoryAsLinkedName").close();
    db.command("CREATE CLASS testMandatoryAsLinkedName_2").close();
    db.command(
            "CREATE PROPERTY testMandatoryAsLinkedName.id EMBEDDEDLIST testMandatoryAsLinkedName_2 UNSAFE")
        .close();

    OClass companyClass = db.getMetadata().getSchema().getClass("testMandatoryAsLinkedName");
    OClass mandatoryClass = db.getMetadata().getSchema().getClass("testMandatoryAsLinkedName_2");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), "testMandatoryAsLinkedName.id");
    assertEquals(idProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(idProperty.getLinkedClass(), mandatoryClass);
    assertFalse(idProperty.isMandatory());
  }

  @Test
  public void testIfNotExists() throws Exception {
    db.command("CREATE class testIfNotExists").close();
    db.command("CREATE property testIfNotExists.name if not exists STRING").close();

    OClass clazz = db.getMetadata().getSchema().getClass("testIfNotExists");
    OProperty nameProperty = clazz.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testIfNotExists.name");
    assertEquals(nameProperty.getType(), OType.STRING);

    db.command("CREATE property testIfNotExists.name if not exists STRING").close();

    clazz = db.getMetadata().getSchema().getClass("testIfNotExists");
    nameProperty = clazz.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), "testIfNotExists.name");
    assertEquals(nameProperty.getType(), OType.STRING);
  }
}
