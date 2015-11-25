/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Michael MacFadden
 */
public class OCommandExecutorSQLCreatePropertyTest {
	
  private static final String PROP_NAME = "name";
  private static final String PROP_FULL_NAME = "company.name";
  private static final String PROP_DIVISION = "division";
  private static final String PROP_FULL_DIVISION = "company.division";
  private static final String PROP_OFFICERS = "officers";
  private static final String PROP_FULL_OFFICERS= "company.officers";

  
  @Test
  public void testBasicCreateProperty() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertEquals(nameProperty.getType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());

    db.close();
  }
  
  @Test
  public void testCreatePropertyWithLinkedClass() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();
    
    db.command(new OCommandSQL("CREATE class division")).execute();
    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.division LINK division")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_DIVISION);

    assertEquals(nameProperty.getName(), PROP_DIVISION);
    assertEquals(nameProperty.getFullName(), PROP_FULL_DIVISION);
    assertEquals(nameProperty.getType(), OType.LINK);
    assertEquals(nameProperty.getLinkedClass().getName(), "division");
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());

    db.close();
  }
  
  @Test
  public void testCreatePropertyWithEmbeddedType() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();
    
    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(new OCommandSQL("CREATE Property company.officers EMBEDDEDLIST STRING")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());

    db.close();
  }
  
  @Test
  public void testCreateMandatoryProperty() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING MANDATORY")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertTrue(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());

    db.close();
  } 
  
  @Test
  public void testCreateNotNullProperty() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING NOTNULL")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertFalse(nameProperty.isMandatory());
    assertTrue(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());

    db.close();
  } 
  
  @Test
  public void testCreateReadOnlyProperty() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();

    db.command(new OCommandSQL("CREATE class company")).execute();
    db.command(new OCommandSQL("CREATE property company.name STRING READONLY")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_NAME);

    assertEquals(nameProperty.getName(), PROP_NAME);
    assertEquals(nameProperty.getFullName(), PROP_FULL_NAME);
    assertFalse(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertTrue(nameProperty.isReadonly());

    db.close();
  }
  
  @Test
  public void testCreateMandatoryPropertyWithEmbeddedType() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();
    
    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(new OCommandSQL("CREATE Property company.officers EMBEDDEDLIST STRING MANDATORY")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertTrue(nameProperty.isMandatory());
    assertFalse(nameProperty.isNotNull());
    assertFalse(nameProperty.isReadonly());

    db.close();
  }
  
  @Test
  public void testCreateUnsafePropertyWithEmbeddedType() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();
    
    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(new OCommandSQL("CREATE Property company.officers EMBEDDEDLIST STRING UNSAFE")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);

    db.close();
  }
  
  @Test
  public void testComplexCreateProperty() throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
      "memory:OCommandExecutorSQLCreatePropertyTest" + System.nanoTime());
    
    db.create();
    
    db.command(new OCommandSQL("CREATE Class company")).execute();
    db.command(new OCommandSQL("CREATE Property company.officers EMBEDDEDLIST STRING MANDATORY READONLY NOTNULL UNSAFE")).execute();

    db.commit();
    db.reload();
    
    OClass companyClass = db.getMetadata().getSchema().getClass("company");
    OProperty nameProperty = companyClass.getProperty(PROP_OFFICERS);

    assertEquals(nameProperty.getName(), PROP_OFFICERS);
    assertEquals(nameProperty.getFullName(), PROP_FULL_OFFICERS);
    assertEquals(nameProperty.getType(), OType.EMBEDDEDLIST);
    assertEquals(nameProperty.getLinkedType(), OType.STRING);
    assertTrue(nameProperty.isMandatory());
    assertTrue(nameProperty.isNotNull());
    assertTrue(nameProperty.isReadonly());

    db.close();
  }
}