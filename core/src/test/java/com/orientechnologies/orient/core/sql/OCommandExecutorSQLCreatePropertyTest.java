/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import org.junit.Test;

/**
 * @author Michael MacFadden
 */
public class OCommandExecutorSQLCreatePropertyTest extends BaseMemoryInternalDatabase {

  private static final String PROP_NAME = "name";
  private static final String PROP_FULL_NAME = "company.name";
  private static final String PROP_DIVISION = "division";
  private static final String PROP_FULL_DIVISION = "company.division";
  private static final String PROP_OFFICERS = "officers";
  private static final String PROP_FULL_OFFICERS = "company.officers";
  private static final String PROP_ID = "id";
  private static final String PROP_FULL_ID = "company.id";

  @Test
  public void testBasicCreateProperty() throws Exception {

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertEquals(nameProperty.getType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testBasicUnsafeCreateProperty() throws Exception {

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING UNSAFE")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertEquals(nameProperty.getType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreatePropertyWithLinkedClass() throws Exception {

    db.command(new OCommandSQL("CREATE class division")).execute();
    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.division LINK division")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_DIVISION);

    assertEquals(nameProperty.getName(), PROP_DIVISION);
    assertEquals(nameProperty.getFullName(), PROP_FULL_DIVISION);
    assertEquals(nameProperty.getType(), OType.LINK);
    assertEquals(nameProperty.getLinkedClass().getName(), "division");
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreatePropertyWithEmbeddedType() throws Exception {

    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(new OCommandSQL("CREATE Property company.officers EMBEDDEDLIST STRING")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateMandatoryProperty() throws Exception {

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING (MANDATORY)")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertTrue(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateNotNullProperty() throws Exception {

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING (NOTNULL)")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertFalse(nameProperty.isMandatory());
    assertTrue(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateReadOnlyProperty() throws Exception {

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING (READONLY)")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertTrue(nameProperty.isReadonly());
  }

  @Test
  public void testCreateReadOnlyFalseProperty() throws Exception {

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING (READONLY false)")).execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateMandatoryPropertyWithEmbeddedType() throws Exception {

    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(new OCommandSQL("CREATE Property company.officers EMBEDDEDLIST STRING (MANDATORY)"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertTrue(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());
  }

  @Test
  public void testCreateUnsafePropertyWithEmbeddedType() throws Exception {

    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(new OCommandSQL("CREATE Property company.officers EMBEDDEDLIST STRING UNSAFE"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
  }

  @Test
  public void testComplexCreateProperty() throws Exception {

    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(
            new OCommandSQL(
                "CREATE Property company.officers EMBEDDEDLIST STRING (MANDATORY, READONLY, NOTNULL) UNSAFE"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertTrue(nameProperty.isMandatory());
    assertTrue(nameProperty.isNotNull());
    assertTrue(nameProperty.isReadonly());
  }

  @Test
  public void testLinkedTypeDefaultAndMinMaxUnsafeProperty() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(
            new OCommandSQL(
                "CREATE PROPERTY company.id EMBEDDEDLIST Integer (DEFAULT 5, MIN 1, MAX 10) UNSAFE"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), PROP_FULL_ID);
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

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(
            new OCommandSQL("CREATE PROPERTY company.id INTEGER (DEFAULT 5, MIN 1, MAX 10) UNSAFE"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), PROP_FULL_ID);
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

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(
            new OCommandSQL(
                "CREATE PROPERTY company.id INTEGER  ( DEFAULT  5 ,  MANDATORY  )  UNSAFE "))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), PROP_FULL_ID);
    assertEquals(idProperty.getType(), OType.INTEGER);
    assertEquals(idProperty.getLinkedType(), null);
    assertTrue(idProperty.isMandatory());
    assertEquals(idProperty.getDefaultValue(), "5");
  }

  @Test
  public void testNonStrict() throws Exception {

    db.getStorage().setProperty(OStatement.CUSTOM_STRICT_SQL, "false");

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(
            new OCommandSQL(
                "CREATE PROPERTY company.id INTEGER (MANDATORY, NOTNULL false, READONLY true, MAX 10, MIN 4, DEFAULT 6)  UNSAFE"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), PROP_FULL_ID);
    assertEquals(idProperty.getType(), OType.INTEGER);
    assertEquals(idProperty.getLinkedType(), null);
    assertTrue(idProperty.isMandatory());
    assertFalse(idProperty.isNotNull());
    assertTrue(idProperty.isReadonly());
    assertEquals(idProperty.getMin(), "4");
    assertEquals(idProperty.getMax(), "10");
    assertEquals(idProperty.getDefaultValue(), "6");
  }

  @Test(expected = OCommandSQLParsingException.class)
  public void testInvalidAttributeName() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(
            new OCommandSQL(
                "CREATE PROPERTY company.id INTEGER (MANDATORY, INVALID, NOTNULL)  UNSAFE"))
        .execute();
  }

  @Test(expected = OCommandSQLParsingException.class)
  public void testMissingAttributeValue() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY company.id INTEGER (DEFAULT)  UNSAFE")).execute();
  }

  @Test(expected = OCommandSQLParsingException.class)
  public void tooManyAttributeParts() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY company.id INTEGER (DEFAULT 5 10)  UNSAFE"))
        .execute();
  }

  @Test
  public void testMandatoryAsLinkedName() throws Exception {

    db.command(new OCommandSQL("CREATE CLASS company")).execute();
    db.command(new OCommandSQL("CREATE CLASS Mandatory")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY company.id EMBEDDEDLIST Mandatory UNSAFE"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OClass mandatoryClass = db.getMetadata().getSchema().getClass("Mandatory");
    OProperty idProperty = companyClass.getProperty(PROP_ID);

    assertEquals(idProperty.getName(), PROP_ID);
    assertEquals(idProperty.getFullName(), PROP_FULL_ID);
    assertEquals(idProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(idProperty.getLinkedClass(), mandatoryClass);
    assertFalse(idProperty.isMandatory());
  }

  @Test
  public void testIfNotExists() throws Exception {

    db.command(new OCommandSQL("CREATE class testIfNotExists")).execute();
    db.command(new OCommandSQL("CREATE property testIfNotExists.name if not exists STRING"))
        .execute();

    OClass companyClass = db.getMetadata().getSchema().getClass("testIfNotExists");
    OProperty property = companyClass.getProperty("name");
    assertEquals(property.getName(), PROP_NAME);

    db.command(new OCommandSQL("CREATE property testIfNotExists.name if not exists STRING"))
        .execute();

    companyClass = db.getMetadata().getSchema().getClass("testIfNotExists");
    property = companyClass.getProperty("name");
    assertEquals(property.getName(), PROP_NAME);
  }
}
