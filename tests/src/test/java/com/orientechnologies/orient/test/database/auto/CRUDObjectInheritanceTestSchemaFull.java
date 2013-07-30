/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import java.lang.reflect.Field;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.test.domain.base.IdObject;
import com.orientechnologies.orient.test.domain.base.Instrument;
import com.orientechnologies.orient.test.domain.base.Musician;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Company;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.inheritance.InheritanceTestAbstractClass;
import com.orientechnologies.orient.test.domain.inheritance.InheritanceTestBaseClass;
import com.orientechnologies.orient.test.domain.inheritance.InheritanceTestClass;
import com.orientechnologies.orient.test.domain.schemageneration.JavaTestSchemaGeneration;
import com.orientechnologies.orient.test.domain.schemageneration.TestSchemaGenerationChild;

@Test(groups = { "crud", "object", "schemafull", "inheritanceSchemaFull" })
public class CRUDObjectInheritanceTestSchemaFull {
  protected static final int TOT_RECORDS = 10;
  protected long             startRecordNumber;
  private OObjectDatabaseTx  database;
  private City               redmond     = new City(new Country("Washington"), "Redmond");

  @Parameters(value = "url")
  public CRUDObjectInheritanceTestSchemaFull(String iURL) {
    database = new OObjectDatabaseTx(iURL + "_objectschema");
    database.create();
    database.close();
  }

  @BeforeClass
  public void init() {

  }

  @Test
  public void create() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");
    startRecordNumber = 0;

    Company company;

    for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
      company = database.newInstance(Company.class, (int) i, "Microsoft" + i);
      company.setEmployees((int) (100000 + i));
      company.getAddresses().add(new Address("Headquarter", redmond, "WA 98073-9717"));
      database.save(company);
    }

    database.close();
  }

  @Test(dependsOnMethods = "create")
  public void testCreate() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);

    Assert.assertEquals(database.countClusterElements("Company") - startRecordNumber, TOT_RECORDS);

    database.close();
  }

  @Test(dependsOnMethods = "testCreate")
  public void queryByBaseType() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);

    final List<Account> result = database.query(new OSQLSynchQuery<Account>("select from Company where name.length() > 0"));

    Assert.assertTrue(result.size() > 0);
    Assert.assertEquals(result.size(), TOT_RECORDS);

    int companyRecords = 0;
    Account account;
    for (int i = 0; i < result.size(); ++i) {
      account = result.get(i);

      if (account instanceof Company)
        companyRecords++;

      Assert.assertNotSame(account.getName().length(), 0);
    }

    Assert.assertEquals(companyRecords, TOT_RECORDS);

    database.close();
  }

  @Test(dependsOnMethods = "queryByBaseType")
  public void queryPerSuperType() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);

    final List<Company> result = database.query(new OSQLSynchQuery<ODocument>("select * from Company where name.length() > 0"));

    Assert.assertTrue(result.size() == TOT_RECORDS);

    Company account;
    for (int i = 0; i < result.size(); ++i) {
      account = result.get(i);
      Assert.assertNotSame(account.getName().length(), 0);
    }

    database.close();
  }

  @Test(dependsOnMethods = "queryPerSuperType")
  public void deleteFirst() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);

    startRecordNumber = database.countClusterElements("Company");

    // DELETE ALL THE RECORD IN THE CLUSTER
    OObjectIteratorCluster<Company> companyClusterIterator = database.browseCluster("Company");
    for (Company obj : companyClusterIterator) {
      if (obj.getId() == 1) {
        database.delete(obj);
        break;
      }
    }

    Assert.assertEquals(database.countClusterElements("Company"), startRecordNumber - 1);

    database.close();
  }

  @Test(dependsOnMethods = "deleteFirst")
  public void testSuperclassInheritanceCreation() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);

    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.inheritance");
    database.close();
    database.open("admin", "admin");
    OClass abstractClass = database.getMetadata().getSchema().getClass(InheritanceTestAbstractClass.class);
    OClass baseClass = database.getMetadata().getSchema().getClass(InheritanceTestBaseClass.class);
    OClass testClass = database.getMetadata().getSchema().getClass(InheritanceTestClass.class);
    Assert.assertEquals(baseClass.getSuperClass(), abstractClass);
    Assert.assertEquals(baseClass.getSuperClass().getName(), abstractClass.getName());
    Assert.assertEquals(testClass.getSuperClass(), baseClass);
    Assert.assertEquals(testClass.getSuperClass().getName(), baseClass.getName());
    database.close();
  }

  @Test(dependsOnMethods = "testSuperclassInheritanceCreation")
  public void testIdFieldInheritance() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);

    database.getEntityManager().registerEntityClass(Musician.class);
    database.getEntityManager().registerEntityClass(Instrument.class);
    database.getEntityManager().registerEntityClass(IdObject.class);
    Field idField = OObjectEntitySerializer.getIdField(IdObject.class);
    Assert.assertNotNull(idField);
    Field musicianIdField = OObjectEntitySerializer.getIdField(Musician.class);
    Assert.assertNotNull(musicianIdField);
    Assert.assertEquals(idField, musicianIdField);
    Field instrumentIdField = OObjectEntitySerializer.getIdField(Instrument.class);
    Assert.assertNotNull(instrumentIdField);
    Assert.assertEquals(idField, instrumentIdField);
    Assert.assertEquals(instrumentIdField, musicianIdField);
    idField = OObjectEntitySerializer.getIdField(IdObject.class);
    database.close();
  }

  @Test(dependsOnMethods = "testIdFieldInheritance")
  public void testIdFieldInheritanceFirstSubClass() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);
    database.command(new OCommandSQL("delete from InheritanceTestBaseClass")).execute();
    database.command(new OCommandSQL("delete from InheritanceTestClass")).execute();

    database.getEntityManager().registerEntityClass(InheritanceTestClass.class);
    database.getEntityManager().registerEntityClass(InheritanceTestBaseClass.class);
    InheritanceTestBaseClass a = database.newInstance(InheritanceTestBaseClass.class);
    InheritanceTestBaseClass b = database.newInstance(InheritanceTestClass.class);
    database.save(a);
    database.save(b);

    final List<InheritanceTestBaseClass> result1 = database.query(new OSQLSynchQuery<InheritanceTestBaseClass>(
        "select from InheritanceTestBaseClass"));
    Assert.assertEquals(2, result1.size());
    database.close();
  }

  @Test(dependsOnMethods = "testIdFieldInheritanceFirstSubClass")
  public void testSchemaGeneration() {
    database.open("admin", "admin");

    database.generateSchema("com.orientechnologies.orient.test.domain.base");
    OClass musicianClass = database.getMetadata().getSchema().getClass(Musician.class);
    OClass instrumentClass = database.getMetadata().getSchema().getClass(Instrument.class);
    checkNotExistsProperty(musicianClass, "id");
    checkNotExistsProperty(musicianClass, "version");
    checkNotExistsProperty(instrumentClass, "id");
    checkNotExistsProperty(instrumentClass, "version");
    checkProperty(musicianClass, "name", OType.STRING);
    checkProperty(musicianClass, "instruments", OType.LINKLIST, instrumentClass);
    database.close();
  }

  @Test(dependsOnMethods = "testSchemaGeneration")
  public void testAutomaticSchemaGeneration() {
    database.open("admin", "admin");
    database.setAutomaticSchemaGeneration(true);

    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.schemageneration");
    OClass testSchemaClass = database.getMetadata().getSchema().getClass(JavaTestSchemaGeneration.class);
    OClass childClass = database.getMetadata().getSchema().getClass(TestSchemaGenerationChild.class);

    checkNotExistsProperty(testSchemaClass, "id");
    checkNotExistsProperty(testSchemaClass, "version");

    // Test simple types
    checkProperty(testSchemaClass, "text", OType.STRING);
    checkProperty(testSchemaClass, "enumeration", OType.STRING);
    checkProperty(testSchemaClass, "numberSimple", OType.INTEGER);
    checkProperty(testSchemaClass, "longSimple", OType.LONG);
    checkProperty(testSchemaClass, "doubleSimple", OType.DOUBLE);
    checkProperty(testSchemaClass, "floatSimple", OType.FLOAT);
    checkProperty(testSchemaClass, "byteSimple", OType.BYTE);
    checkProperty(testSchemaClass, "flagSimple", OType.BOOLEAN);
    checkProperty(testSchemaClass, "dateField", OType.DATETIME);

    // Test complex types
    checkProperty(testSchemaClass, "stringListMap", OType.EMBEDDEDMAP, OType.EMBEDDEDLIST);
    checkProperty(testSchemaClass, "enumList", OType.EMBEDDEDLIST, OType.STRING);
    checkProperty(testSchemaClass, "enumSet", OType.EMBEDDEDSET, OType.STRING);
    checkProperty(testSchemaClass, "stringSet", OType.EMBEDDEDSET, OType.STRING);
    checkProperty(testSchemaClass, "stringMap", OType.EMBEDDEDMAP, OType.STRING);
    checkProperty(testSchemaClass, "enumMap", OType.EMBEDDEDMAP, OType.STRING);

    // Test linked types
    checkNotExistsProperty(testSchemaClass, "document");
    checkNotExistsProperty(testSchemaClass, "mapObject ");
    checkNotExistsProperty(testSchemaClass, "byteArray");
    checkProperty(testSchemaClass, "list", OType.LINKLIST, childClass);
    checkProperty(testSchemaClass, "set", OType.LINKSET, childClass);
    checkProperty(testSchemaClass, "children", OType.LINKMAP, childClass);
    checkProperty(testSchemaClass, "child", OType.LINK, childClass);
    checkProperty(testSchemaClass, "enumMap", OType.EMBEDDEDMAP, OType.STRING);

    // Test embedded types
    checkNotExistsProperty(testSchemaClass, "embeddedDocument");
    checkProperty(testSchemaClass, "embeddedSet", OType.EMBEDDEDSET, childClass);
    checkProperty(testSchemaClass, "embeddedChildren", OType.EMBEDDEDMAP, childClass);
    checkProperty(testSchemaClass, "embeddedChild", OType.EMBEDDED, childClass);
    checkProperty(testSchemaClass, "embeddedList", OType.EMBEDDEDLIST, childClass);

    database.setAutomaticSchemaGeneration(false);
    database.close();
  }

  @Test(dependsOnMethods = "testAutomaticSchemaGeneration")
  public void testMultipleSchemaGeneration() {
    database.open("admin", "admin");
    try {
      database.generateSchema(Musician.class);
      database.generateSchema(JavaTestSchemaGeneration.class);
      database.generateSchema(TestSchemaGenerationChild.class);
    } catch (Exception e) {
      Assert.fail("Shouldn't throw exceptions");
    }
    database.close();
  }

  protected void checkNotExistsProperty(OClass iClass, String iPropertyName) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNull(prop);
  }

  protected void checkProperty(OClass iClass, String iPropertyName, OType iType) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
  }

  protected void checkProperty(OClass iClass, String iPropertyName, OType iType, OClass iLinkedClass) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
    Assert.assertEquals(prop.getLinkedClass(), iLinkedClass);
  }

  protected void checkProperty(OClass iClass, String iPropertyName, OType iType, OType iLinkedType) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertEquals(prop.getType(), iType);
    Assert.assertEquals(prop.getLinkedType(), iLinkedType);
  }

  protected void checkNoLinkedProperty(OClass iClass, String iPropertyName, OType iType) {
    OProperty prop = iClass.getProperty(iPropertyName);
    Assert.assertNotNull(prop);
    Assert.assertNull(prop.getLinkedType());
    Assert.assertNull(prop.getLinkedClass());
  }
}
