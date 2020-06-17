/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.domain.base.EnumTest;
import com.orientechnologies.orient.test.domain.base.JavaAttachDetachTestClass;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.Child;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javassist.util.proxy.Proxy;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(
    groups = {"object", "detachingSchemaFull"},
    dependsOnGroups = "treeSchemaFull")
public class ObjectDetachingTestSchemaFull extends ObjectDBBaseTest {
  private Account account;
  private Profile profile;

  @Parameters(value = "url")
  public ObjectDetachingTestSchemaFull(@Optional String url) {
    super(url, "_objectschema");
  }

  @Test
  public void createAnnotatedObjects() {
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.base");
    Country austria = new Country("Austria");
    City graz = new City(austria, "Graz");
    graz = database.save(graz);

    account = new Account();
    account = database.save(account);

    profile = new Profile();
    profile = database.save(profile);
  }

  @Test(dependsOnMethods = "createAnnotatedObjects")
  public void testJPALongIdAnnotation() {
    // BROWSE ALL THE OBJECTS
    Assert.assertTrue(database.countClass(City.class) > 0);
    for (City c : database.browseClass(City.class)) {
      c.getId();
      Assert.assertNotNull(c.getId());
    }
  }

  @Test(dependsOnMethods = "testJPALongIdAnnotation")
  public void testJPAObjectIdAnnotation() {
    // BROWSE ALL THE OBJECTS
    Assert.assertTrue(database.countClass(Account.class) > 0);
    for (Account a : database.browseClass(Account.class)) {
      Assert.assertNotNull(a.getRid());
    }
  }

  @Test(dependsOnMethods = "testJPAObjectIdAnnotation")
  public void testOrientObjectIdAnnotation() {
    // BROWSE ALL THE OBJECTS
    Assert.assertTrue(database.countClass(Country.class) > 0);
    for (Country c : database.browseClass(Country.class)) {
      Assert.assertNotNull(c.getId());
    }
  }

  @Test(dependsOnMethods = "testOrientObjectIdAnnotation")
  public void testOrientStringIdAnnotation() {
    // BROWSE ALL THE OBJECTS
    Assert.assertTrue(database.countClass(Profile.class) > 0);
    for (Profile a : database.browseClass(Profile.class)) {
      Assert.assertNotNull(a.getId());
    }
  }

  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "testOrientStringIdAnnotation")
  public void testOrientObjectIdPlusVersionAnnotationsNotInTx() {
    // DELETE PREVIOUS TESTS DATA
    database.command(new OCommandSQL("delete from Country where name = 'Austria v1'")).execute();
    // BROWSE ALL THE OBJECTS
    Assert.assertTrue(database.countClass(Country.class) > 0);
    for (Country c :
        (List<Country>)
            database.query(
                new OSQLSynchQuery<Object>("select from Country where name = 'Austria'"))) {
      Assert.assertNotNull(c.getId());
      Assert.assertNotNull(c.getVersion());

      // UPDATE IT TO GET NEWER VERSION
      c.setName(c.getName() + " v1");
      database.save(c);

      // CHECK VERSION
      Assert.assertTrue(((Integer) c.getVersion()) > 0);
    }

    // BROWSE ALL THE OBJECTS
    for (Country c :
        (List<Country>)
            database.query(
                new OSQLSynchQuery<Object>("select from Country where name = 'Austria v1'"))) {
      Assert.assertNotNull(c.getId());
      Assert.assertNotNull(c.getVersion());
      Assert.assertTrue(((Integer) c.getVersion()) > 0);
    }
  }

  @Test(
      dependsOnMethods =
          "testOrientObjectIdPlusVersionAnnotationsNotInTx" /* , expectedExceptions = OTransactionException.class */)
  public void testOrientObjectIdPlusVersionAnnotationsInTx() {
    // TODO CHECK WHY SHOULD THROW EXCEPTION
    // database.begin();
    //
    // try {
    // // BROWSE ALL THE OBJECTS
    // Assert.assertTrue(database.countClass(Account.class) > 0);
    // for (Account a : database.browseClass(Account.class)) {
    // Assert.assertNotNull(a.getId());
    //
    // // UPDATE IT TO GET NEWER VERSION
    // a.setName(a.getName() + " v1");
    // database.save(a);
    // break;
    // }
    //
    // database.commit();
    //
    // Assert.assertTrue(false);
    // } finally {
    // database.rollback();
    // }
  }

  @Test(dependsOnMethods = "testOrientObjectIdPlusVersionAnnotationsInTx")
  public void testInsertCommit() {
    String initialCountryName = "insertCommit";
    Country country = new Country(initialCountryName);

    long initCount = database.countClass(Country.class);

    database.begin();
    country = (Country) database.save(country);
    database.commit();

    Assert.assertEquals(database.countClass(Country.class), initCount + 1);
    Assert.assertNotNull(country.getId());
    Assert.assertNotNull(country.getVersion());

    Country found = (Country) database.load((ORecordId) country.getId());
    Assert.assertNotNull(found);
    Assert.assertEquals(country.getName(), found.getName());
  }

  @Test(dependsOnMethods = "testInsertCommit")
  public void testInsertRollback() {
    String initialCountryName = "insertRollback";
    Country country = new Country(initialCountryName);

    long initCount = database.countClass(Country.class);

    database.begin();
    country = (Country) database.save(country);
    database.rollback();

    Assert.assertEquals(database.countClass(Country.class), initCount);
    Assert.assertTrue(
        country.getId() == null || ((ORID) country.getId()).isNew(), "id=" + country.getId());
    // Assert.assertNull(country.getVersion());
  }

  @Test(dependsOnMethods = "testInsertRollback")
  public void testUpdateCommit() {
    String initialCountryName = "updateCommit";
    Country country = new Country(initialCountryName);

    country = (Country) database.save(country);
    Assert.assertNotNull(country.getId());
    Assert.assertNotNull(country.getVersion());

    int initVersion = ((Integer) country.getVersion());

    database.begin();
    Country loaded = (Country) database.load((ORecordId) country.getId());
    Assert.assertEquals(loaded.getId(), country.getId());
    Assert.assertEquals(loaded.getVersion(), country.getVersion());
    Assert.assertEquals(
        (Object) database.getRecordByUserObject(loaded, false),
        (Object) database.getRecordByUserObject(country, false));
    String newName = "ShouldBeChanged";
    loaded.setName(newName);
    loaded = (Country) database.save(loaded);
    database.commit();

    loaded = (Country) database.load((ORecordId) country.getId());
    Assert.assertEquals(
        (Object) database.getRecordByUserObject(loaded, false),
        (Object) database.getRecordByUserObject(country, false));
    Assert.assertEquals(loaded.getId(), country.getId());
    Assert.assertEquals((int) (Integer) loaded.getVersion(), initVersion + 1);
    Assert.assertEquals(loaded.getName(), newName);
  }

  @Test(dependsOnMethods = "testUpdateCommit")
  public void testUpdateRollback() {
    String initialCountryName = "updateRollback";
    Country country = new Country(initialCountryName);

    country = (Country) database.save(country);
    Assert.assertNotNull(country.getId());
    Assert.assertNotNull(country.getVersion());

    int initVersion = (Integer) country.getVersion();

    database.begin();
    Country loaded = (Country) database.load((ORecordId) country.getId());
    Assert.assertEquals(loaded.getId(), country.getId());
    Assert.assertEquals(loaded.getVersion(), country.getVersion());
    Assert.assertEquals(
        (Object) database.getRecordByUserObject(loaded, false),
        database.getRecordByUserObject(country, false));
    String newName = "ShouldNotBeChanged";
    loaded.setName(newName);
    loaded = (Country) database.save(loaded);
    database.rollback();

    loaded = database.load((ORecordId) country.getId());
    Assert.assertNotSame(
        database.getRecordByUserObject(loaded, false),
        database.getRecordByUserObject(country, false));
    Assert.assertEquals((Integer) loaded.getVersion(), (Integer) initVersion);
    Assert.assertEquals(loaded.getName(), initialCountryName);
  }

  @Test(dependsOnMethods = "testUpdateRollback")
  public void testDeleteCommit() {
    String initialCountryName = "deleteCommit";
    Country country = new Country(initialCountryName);

    long initCount = database.countClass(Country.class);

    country = database.save(country);

    Assert.assertEquals(database.countClass(Country.class), initCount + 1);

    database.begin();
    database.delete(country);
    database.commit();

    Assert.assertEquals(database.countClass(Country.class), initCount);
    Country found = database.load((ORecordId) country.getId());
    Assert.assertNull(found);
  }

  @Test(dependsOnMethods = "testDeleteCommit")
  public void testDeleteRollback() {
    String initialCountryName = "deleteRollback";
    Country country = new Country(initialCountryName);

    long initCount = database.countClass(Country.class);

    country = database.save(country);

    Assert.assertEquals(database.countClass(Country.class), initCount + 1);

    database.begin();
    database.delete(country);
    database.rollback();

    Assert.assertEquals(database.countClass(Country.class), initCount + 1);
    Country found = database.load((ORecordId) country.getId());
    Assert.assertNotNull(found);
    Assert.assertEquals(found.getName(), country.getName());
  }

  @Test(dependsOnMethods = "testDeleteRollback")
  public void clean() {
    database.close();

    database = new OObjectDatabaseTx(url).open("admin", "admin");
    database.delete(profile);
    database.delete(account);
  }

  @Test(dependsOnMethods = "clean")
  public void testAttachDetach() {
    JavaAttachDetachTestClass attach = database.newInstance(JavaAttachDetachTestClass.class);
    attach.text = "test";
    attach.numberSimple = 12345;
    attach.doubleSimple = 12.34d;
    attach.floatSimple = 123.45f;
    attach.longSimple = 12345678l;
    attach.byteSimple = (byte) 1;
    attach.flagSimple = true;
    attach.enumeration = EnumTest.ENUM1;
    attach.testTransient = "11";
    Child c = database.newInstance(Child.class);
    c.setName("Jesus");

    attach.children = new HashMap<String, Child>();
    attach.children.put("first", c);

    attach.enumList = new ArrayList<EnumTest>();
    attach.enumList.add(EnumTest.ENUM1);
    attach.enumList.add(EnumTest.ENUM2);

    attach.enumSet = new HashSet<EnumTest>();
    attach.enumSet.add(EnumTest.ENUM1);
    attach.enumSet.add(EnumTest.ENUM3);

    attach.enumMap = new HashMap<String, EnumTest>();
    attach.enumMap.put("1", EnumTest.ENUM2);
    attach.enumMap.put("2", EnumTest.ENUM3);
    database.attach(attach);
    ODocument doc = database.getRecordByUserObject(attach, false);
    Assert.assertTrue(!doc.containsField("testStatic"));
    Assert.assertTrue(!doc.containsField("testTransient"));
    Assert.assertEquals(doc.field("text"), "test");
    Assert.assertEquals(doc.<Object>field("numberSimple"), 12345);
    Assert.assertEquals(doc.field("doubleSimple"), 12.34d);
    Assert.assertEquals(doc.field("floatSimple"), 123.45f);
    Assert.assertEquals(doc.<Object>field("longSimple"), 12345678l);
    Assert.assertEquals(doc.<Object>field("byteSimple"), (byte) 1);
    Assert.assertEquals(doc.<Object>field("flagSimple"), true);
    Assert.assertEquals(doc.field("enumeration"), EnumTest.ENUM1.toString());
    Assert.assertTrue(doc.field("children") instanceof Map<?, ?>);
    Assert.assertTrue(((Map<?, ?>) doc.field("children")).get("first") instanceof ODocument);
    Assert.assertEquals(
        ((ODocument) ((Map<?, ?>) doc.field("children")).get("first")).field("name"), "Jesus");
    Assert.assertEquals(((List<?>) doc.field("enumList")).size(), 2);
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(0), EnumTest.ENUM1.toString());
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(1), EnumTest.ENUM2.toString());

    Assert.assertEquals(((Set<?>) doc.field("enumSet")).size(), 2);
    Iterator<?> it = ((Set<?>) doc.field("enumSet")).iterator();
    Assert.assertEquals(it.next(), EnumTest.ENUM1.toString());
    Assert.assertEquals(it.next(), EnumTest.ENUM3.toString());

    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).size(), 2);
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("1"), EnumTest.ENUM2.toString());
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("2"), EnumTest.ENUM3.toString());

    JavaAttachDetachTestClass savedJavaObj = database.save(attach);

    ORecordId id = (ORecordId) database.getRecordByUserObject(savedJavaObj, false).getIdentity();
    database.close();

    database = new OObjectDatabaseTx(url).open("admin", "admin");
    JavaAttachDetachTestClass loadedJavaObj = (JavaAttachDetachTestClass) database.load(id);
    database.detach(loadedJavaObj);
    Assert.assertEquals(loadedJavaObj.text, "test");
    Assert.assertEquals(loadedJavaObj.numberSimple, 12345);
    Assert.assertEquals(loadedJavaObj.doubleSimple, 12.34d);
    Assert.assertEquals(loadedJavaObj.floatSimple, 123.45f);
    Assert.assertEquals(loadedJavaObj.longSimple, 12345678l);
    Assert.assertEquals(loadedJavaObj.byteSimple, (byte) 1);
    Assert.assertEquals(loadedJavaObj.flagSimple, true);
    Assert.assertEquals(loadedJavaObj.enumeration, EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumList.get(0), EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.get(1), EnumTest.ENUM2);

    Assert.assertEquals(loadedJavaObj.enumSet.size(), 2);
    it = loadedJavaObj.enumSet.iterator();
    Assert.assertEquals(it.next(), EnumTest.ENUM1);
    Assert.assertEquals(it.next(), EnumTest.ENUM3);

    Assert.assertEquals(loadedJavaObj.enumMap.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("1"), EnumTest.ENUM2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("2"), EnumTest.ENUM3);
  }

  @Test(dependsOnMethods = "testAttachDetach")
  public void testDetachAll() {
    JavaAttachDetachTestClass attach = database.newInstance(JavaAttachDetachTestClass.class);
    attach.text = "test";
    attach.numberSimple = 12345;
    attach.doubleSimple = 12.34d;
    attach.floatSimple = 123.45f;
    attach.longSimple = 12345678l;
    attach.byteSimple = (byte) 1;
    attach.flagSimple = true;
    attach.enumeration = EnumTest.ENUM1;
    Child c = database.newInstance(Child.class);
    c.setName("Jesus");

    attach.children = new HashMap<String, Child>();
    attach.children.put("first", c);

    attach.enumList = new ArrayList<EnumTest>();
    attach.enumList.add(EnumTest.ENUM1);
    attach.enumList.add(EnumTest.ENUM2);

    attach.enumSet = new HashSet<EnumTest>();
    attach.enumSet.add(EnumTest.ENUM1);
    attach.enumSet.add(EnumTest.ENUM3);

    attach.enumMap = new HashMap<String, EnumTest>();
    attach.enumMap.put("1", EnumTest.ENUM2);
    attach.enumMap.put("2", EnumTest.ENUM3);
    database.attach(attach);
    ODocument doc = database.getRecordByUserObject(attach, false);
    Assert.assertEquals(doc.field("text"), "test");
    Assert.assertEquals(doc.<Object>field("numberSimple"), 12345);
    Assert.assertEquals(doc.field("doubleSimple"), 12.34d);
    Assert.assertEquals(doc.field("floatSimple"), 123.45f);
    Assert.assertEquals(doc.<Object>field("longSimple"), 12345678l);
    Assert.assertEquals(doc.<Object>field("byteSimple"), (byte) 1);
    Assert.assertEquals(doc.<Object>field("flagSimple"), true);
    Assert.assertEquals(doc.field("enumeration"), EnumTest.ENUM1.toString());
    Assert.assertTrue(doc.field("children") instanceof Map<?, ?>);
    Assert.assertTrue(((Map<?, ?>) doc.field("children")).get("first") instanceof ODocument);
    Assert.assertEquals(
        ((ODocument) ((Map<?, ?>) doc.field("children")).get("first")).field("name"), "Jesus");
    Assert.assertEquals(((List<?>) doc.field("enumList")).size(), 2);
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(0), EnumTest.ENUM1.toString());
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(1), EnumTest.ENUM2.toString());

    Assert.assertEquals(((Set<?>) doc.field("enumSet")).size(), 2);
    Iterator<?> it = ((Set<?>) doc.field("enumSet")).iterator();
    Assert.assertEquals(it.next(), EnumTest.ENUM1.toString());
    Assert.assertEquals(it.next(), EnumTest.ENUM3.toString());

    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).size(), 2);
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("1"), EnumTest.ENUM2.toString());
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("2"), EnumTest.ENUM3.toString());

    JavaAttachDetachTestClass savedJavaObj = database.save(attach);

    ORecordId id = (ORecordId) database.getRecordByUserObject(savedJavaObj, false).getIdentity();
    database.close();

    database = new OObjectDatabaseTx(url).open("admin", "admin");
    JavaAttachDetachTestClass loadedJavaObj = (JavaAttachDetachTestClass) database.load(id);
    database.detachAll(loadedJavaObj, false);
    Assert.assertEquals(loadedJavaObj.text, "test");
    Assert.assertEquals(loadedJavaObj.numberSimple, 12345);
    Assert.assertEquals(loadedJavaObj.doubleSimple, 12.34d);
    Assert.assertEquals(loadedJavaObj.floatSimple, 123.45f);
    Assert.assertEquals(loadedJavaObj.longSimple, 12345678l);
    Assert.assertEquals(loadedJavaObj.byteSimple, (byte) 1);
    Assert.assertEquals(loadedJavaObj.flagSimple, true);
    Assert.assertEquals(loadedJavaObj.enumeration, EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumList.get(0), EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.get(1), EnumTest.ENUM2);

    Assert.assertTrue(loadedJavaObj.children instanceof Map<?, ?>);
    Assert.assertTrue(loadedJavaObj.children.get("first") instanceof Child);
    Assert.assertEquals(loadedJavaObj.children.get("first").getName(), "Jesus");

    Assert.assertEquals(loadedJavaObj.enumSet.size(), 2);
    it = loadedJavaObj.enumSet.iterator();
    Assert.assertEquals(it.next(), EnumTest.ENUM1);
    Assert.assertEquals(it.next(), EnumTest.ENUM3);

    Assert.assertEquals(loadedJavaObj.enumMap.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("1"), EnumTest.ENUM2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("2"), EnumTest.ENUM3);
  }

  @Test(dependsOnMethods = "testDetachAll")
  public void testNonProxiedAttachDetach() {
    JavaAttachDetachTestClass attach = database.newInstance(JavaAttachDetachTestClass.class);
    attach.text = "test";
    attach.numberSimple = 12345;
    attach.doubleSimple = 12.34d;
    attach.floatSimple = 123.45f;
    attach.longSimple = 12345678l;
    attach.byteSimple = (byte) 1;
    attach.flagSimple = true;
    attach.enumeration = EnumTest.ENUM1;
    Child c = database.newInstance(Child.class);
    c.setName("Jesus");

    attach.children = new HashMap<String, Child>();
    attach.children.put("first", c);

    attach.enumList = new ArrayList<EnumTest>();
    attach.enumList.add(EnumTest.ENUM1);
    attach.enumList.add(EnumTest.ENUM2);

    attach.enumSet = new HashSet<EnumTest>();
    attach.enumSet.add(EnumTest.ENUM1);
    attach.enumSet.add(EnumTest.ENUM3);

    attach.enumMap = new HashMap<String, EnumTest>();
    attach.enumMap.put("1", EnumTest.ENUM2);
    attach.enumMap.put("2", EnumTest.ENUM3);
    database.attach(attach);
    ODocument doc = database.getRecordByUserObject(attach, false);
    Assert.assertEquals(doc.field("text"), "test");
    Assert.assertEquals(doc.<Object>field("numberSimple"), 12345);
    Assert.assertEquals(doc.field("doubleSimple"), 12.34d);
    Assert.assertEquals(doc.field("floatSimple"), 123.45f);
    Assert.assertEquals(doc.<Object>field("longSimple"), 12345678l);
    Assert.assertEquals(doc.<Object>field("byteSimple"), (byte) 1);
    Assert.assertEquals(doc.<Object>field("flagSimple"), true);
    Assert.assertEquals(doc.field("enumeration"), EnumTest.ENUM1.toString());
    Assert.assertTrue(doc.field("children") instanceof Map<?, ?>);
    Assert.assertTrue(((Map<?, ?>) doc.field("children")).get("first") instanceof ODocument);
    Assert.assertEquals(
        ((ODocument) ((Map<?, ?>) doc.field("children")).get("first")).field("name"), "Jesus");
    Assert.assertEquals(((List<?>) doc.field("enumList")).size(), 2);
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(0), EnumTest.ENUM1.toString());
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(1), EnumTest.ENUM2.toString());

    Assert.assertEquals(((Set<?>) doc.field("enumSet")).size(), 2);
    Iterator<?> it = ((Set<?>) doc.field("enumSet")).iterator();
    Assert.assertEquals(it.next(), EnumTest.ENUM1.toString());
    Assert.assertEquals(it.next(), EnumTest.ENUM3.toString());

    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).size(), 2);
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("1"), EnumTest.ENUM2.toString());
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("2"), EnumTest.ENUM3.toString());

    JavaAttachDetachTestClass savedJavaObj = database.save(attach);

    ORecordId id = (ORecordId) database.getRecordByUserObject(savedJavaObj, false).getIdentity();
    database.close();

    database = new OObjectDatabaseTx(url).open("admin", "admin");
    JavaAttachDetachTestClass loadedJavaObj = (JavaAttachDetachTestClass) database.load(id);
    loadedJavaObj = database.detach(loadedJavaObj, true);
    Assert.assertTrue(!(loadedJavaObj instanceof Proxy));
    Assert.assertEquals(loadedJavaObj.text, "test");
    Assert.assertEquals(loadedJavaObj.numberSimple, 12345);
    Assert.assertEquals(loadedJavaObj.doubleSimple, 12.34d);
    Assert.assertEquals(loadedJavaObj.floatSimple, 123.45f);
    Assert.assertEquals(loadedJavaObj.longSimple, 12345678l);
    Assert.assertEquals(loadedJavaObj.byteSimple, (byte) 1);
    Assert.assertEquals(loadedJavaObj.flagSimple, true);
    Assert.assertEquals(loadedJavaObj.enumeration, EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumList.get(0), EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.get(1), EnumTest.ENUM2);

    Assert.assertEquals(loadedJavaObj.enumSet.size(), 2);
    it = loadedJavaObj.enumSet.iterator();
    EnumTest next = (EnumTest) it.next();
    Assert.assertTrue(next.equals(EnumTest.ENUM1) || next.equals(EnumTest.ENUM3));
    next = (EnumTest) it.next();
    Assert.assertTrue(next.equals(EnumTest.ENUM1) || next.equals(EnumTest.ENUM3));
    ;

    Assert.assertEquals(loadedJavaObj.enumMap.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("1"), EnumTest.ENUM2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("2"), EnumTest.ENUM3);
    ODocument serializedDoc = database.getRecordByUserObject(loadedJavaObj, false);
    Assert.assertTrue(serializedDoc.equals(doc));
    Assert.assertTrue(serializedDoc.hasSameContentOf(doc));
  }

  @Test(dependsOnMethods = "testNonProxiedAttachDetach")
  public void testDetachAllNonProxied() {
    JavaAttachDetachTestClass attach = database.newInstance(JavaAttachDetachTestClass.class);
    attach.text = "test";
    attach.numberSimple = 12345;
    attach.doubleSimple = 12.34d;
    attach.floatSimple = 123.45f;
    attach.longSimple = 12345678l;
    attach.byteSimple = (byte) 1;
    attach.flagSimple = true;
    attach.enumeration = EnumTest.ENUM1;
    Child c = database.newInstance(Child.class);
    c.setName("Jesus");

    attach.children = new HashMap<String, Child>();
    attach.children.put("first", c);

    attach.enumList = new ArrayList<EnumTest>();
    attach.enumList.add(EnumTest.ENUM1);
    attach.enumList.add(EnumTest.ENUM2);

    attach.enumSet = new HashSet<EnumTest>();
    attach.enumSet.add(EnumTest.ENUM1);
    attach.enumSet.add(EnumTest.ENUM3);

    attach.enumMap = new HashMap<String, EnumTest>();
    attach.enumMap.put("1", EnumTest.ENUM2);
    attach.enumMap.put("2", EnumTest.ENUM3);
    database.attach(attach);
    ODocument doc = database.getRecordByUserObject(attach, false);
    Assert.assertEquals(doc.field("text"), "test");
    Assert.assertEquals(doc.<Object>field("numberSimple"), 12345);
    Assert.assertEquals(doc.field("doubleSimple"), 12.34d);
    Assert.assertEquals(doc.field("floatSimple"), 123.45f);
    Assert.assertEquals(doc.<Object>field("longSimple"), 12345678l);
    Assert.assertEquals(doc.<Object>field("byteSimple"), (byte) 1);
    Assert.assertEquals(doc.<Object>field("flagSimple"), true);
    Assert.assertEquals(doc.field("enumeration"), EnumTest.ENUM1.toString());
    Assert.assertTrue(doc.field("children") instanceof Map<?, ?>);
    Assert.assertTrue(((Map<?, ?>) doc.field("children")).get("first") instanceof ODocument);
    Assert.assertEquals(
        ((ODocument) ((Map<?, ?>) doc.field("children")).get("first")).field("name"), "Jesus");
    Assert.assertEquals(((List<?>) doc.field("enumList")).size(), 2);
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(0), EnumTest.ENUM1.toString());
    Assert.assertEquals(((List<?>) doc.field("enumList")).get(1), EnumTest.ENUM2.toString());

    Assert.assertEquals(((Set<?>) doc.field("enumSet")).size(), 2);
    Iterator<?> it = ((Set<?>) doc.field("enumSet")).iterator();
    Assert.assertEquals(it.next(), EnumTest.ENUM1.toString());
    Assert.assertEquals(it.next(), EnumTest.ENUM3.toString());

    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).size(), 2);
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("1"), EnumTest.ENUM2.toString());
    Assert.assertEquals(((Map<?, ?>) doc.field("enumMap")).get("2"), EnumTest.ENUM3.toString());

    JavaAttachDetachTestClass savedJavaObj = database.save(attach);

    ORecordId id = (ORecordId) database.getRecordByUserObject(savedJavaObj, false).getIdentity();
    database.close();

    database = new OObjectDatabaseTx(url).open("admin", "admin");
    JavaAttachDetachTestClass loadedJavaObj = (JavaAttachDetachTestClass) database.load(id);
    loadedJavaObj = database.detachAll(loadedJavaObj, true);
    Assert.assertTrue(!(loadedJavaObj instanceof Proxy));
    Assert.assertEquals(loadedJavaObj.text, "test");
    Assert.assertEquals(loadedJavaObj.numberSimple, 12345);
    Assert.assertEquals(loadedJavaObj.doubleSimple, 12.34d);
    Assert.assertEquals(loadedJavaObj.floatSimple, 123.45f);
    Assert.assertEquals(loadedJavaObj.longSimple, 12345678l);
    Assert.assertEquals(loadedJavaObj.byteSimple, (byte) 1);
    Assert.assertEquals(loadedJavaObj.flagSimple, true);
    Assert.assertEquals(loadedJavaObj.enumeration, EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumList.get(0), EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.enumList.get(1), EnumTest.ENUM2);

    Assert.assertTrue(loadedJavaObj.children instanceof Map<?, ?>);
    Assert.assertTrue(loadedJavaObj.children.get("first") instanceof Child);
    Assert.assertTrue(!(loadedJavaObj.children.get("first") instanceof Proxy));
    Assert.assertEquals(loadedJavaObj.children.get("first").getName(), "Jesus");

    Assert.assertEquals(loadedJavaObj.enumSet.size(), 2);
    it = loadedJavaObj.enumSet.iterator();
    EnumTest next = (EnumTest) it.next();
    Assert.assertTrue(next.equals(EnumTest.ENUM1) || next.equals(EnumTest.ENUM3));
    next = (EnumTest) it.next();
    Assert.assertTrue(next.equals(EnumTest.ENUM1) || next.equals(EnumTest.ENUM3));

    Assert.assertEquals(loadedJavaObj.enumMap.size(), 2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("1"), EnumTest.ENUM2);
    Assert.assertEquals(loadedJavaObj.enumMap.get("2"), EnumTest.ENUM3);
  }

  @Test(dependsOnMethods = "testDetachAllNonProxied")
  public void testReloadAndDetachAll() {
    // DELETE PREVIOUS TEST DATA
    database.command(new OCommandSQL("delete from Profile where nick = 'Jack'")).execute();
    // Open db
    // Create the address without country
    Address anAddress = new Address("Godewaersvelde");
    anAddress = database.save(anAddress);
    Address realAddress = database.detachAll(anAddress, true);
    // Create the person
    Profile aPerson = new Profile("Jack", "Jack", "Black", null);
    aPerson.setLocation(realAddress);
    aPerson = database.save(aPerson);
    // Update the address by another way (another process for example)
    City aCity = new City("Paris");
    aCity = database.save(aCity);
    String command = "update " + anAddress.getId() + " set city = " + database.getIdentity(aCity);
    database.command(new OCommandSQL(command)).execute();
    realAddress = database.reload(anAddress, true);
    Assert.assertNotNull(realAddress.getCity());
    // At this point, in OrientDB Studio everything is fine
    // The address has the good country @rid, with version +1
    // Now reload and detachAll the person
    Profile newPerson = database.reload(aPerson, "*:-1", true);
    Profile finalPerson = database.detachAll(newPerson, true);
    // But with the reload, the country is null
    Assert.assertNotNull(finalPerson.getLocation().getCity()); // out = null
    // Same problem with query and detachAll
    String query = "select from Profile where name = 'Jack' and surname = 'Black'";
    newPerson = (Profile) database.query(new OSQLSynchQuery<Object>(query), new Object[0]).get(0);
    finalPerson = database.detachAll(newPerson, true);
    Assert.assertNotNull(finalPerson.getLocation().getCity()); // out = null
    // Close db
  }

  @Test(dependsOnMethods = "testReloadAndDetachAll")
  public void testObjectSerialization() {
    Profile profile =
        new Profile(
            "NonProxiedObjectToDelete",
            "NonProxiedObjectToDelete",
            "NonProxiedObjectToDelete",
            null);
    profile = database.save(profile);
    ODocument originalDoc = database.getRecordByUserObject(profile, false);
    // DETACH TEST
    ODocument serializedDoc = database.getRecordByUserObject(database.detach(profile, true), false);
    Assert.assertTrue(originalDoc.equals(serializedDoc));
    Assert.assertTrue(originalDoc.hasSameContentOf(serializedDoc));

    // DETACH ALL TEST
    serializedDoc = database.getRecordByUserObject(database.detachAll(profile, true), false);
    Assert.assertTrue(originalDoc.equals(serializedDoc));
    Assert.assertTrue(originalDoc.hasSameContentOf(serializedDoc));
    database.delete(profile);
  }
}
