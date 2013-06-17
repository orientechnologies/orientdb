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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javassist.util.proxy.Proxy;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.object.OLazyObjectSetInterface;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.object.db.ODatabaseObjectTx;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.iterator.OObjectIteratorClass;
import com.orientechnologies.orient.object.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.test.domain.base.EmbeddedChild;
import com.orientechnologies.orient.test.domain.base.EnumTest;
import com.orientechnologies.orient.test.domain.base.IdObject;
import com.orientechnologies.orient.test.domain.base.Instrument;
import com.orientechnologies.orient.test.domain.base.JavaComplexTestClass;
import com.orientechnologies.orient.test.domain.base.JavaSimpleTestClass;
import com.orientechnologies.orient.test.domain.base.JavaTestInterface;
import com.orientechnologies.orient.test.domain.base.Musician;
import com.orientechnologies.orient.test.domain.base.Parent;
import com.orientechnologies.orient.test.domain.base.PersonTest;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.Child;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "crud", "object" })
public class CRUDObjectPhysicalTest {
  protected static final int TOT_RECORDS = 100;
  protected long             startRecordNumber;
  private OObjectDatabaseTx  database;
  private City               rome        = new City(new Country("Italy"), "Rome");
  private String             url;

  @Parameters(value = "url")
  public CRUDObjectPhysicalTest(String iURL) {
    url = iURL;
  }

  @Test
  public void create() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");

    startRecordNumber = database.countClusterElements("Account");

    Account account;

    for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
      account = new Account((int) i, "Bill", "Gates");
      account.setBirthDate(new Date());
      account.setSalary(i + 300.10f);
      account.getAddresses().add(new Address("Residence", rome, "Piazza Navona, 1"));
      database.save(account);
    }

    database.close();
  }

  @Test(dependsOnMethods = "create", expectedExceptions = UnsupportedOperationException.class)
  public void testReleasedPoolDatabase() {
    database.open("admin", "admin");
  }

  @Test(dependsOnMethods = "testReleasedPoolDatabase")
  public void testCreate() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    Assert.assertEquals(database.countClusterElements("Account") - startRecordNumber, TOT_RECORDS);

    database.close();
  }

  @Test(dependsOnMethods = "testCreate")
  public void testAutoCreateClass() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    Assert.assertNull(database.getMetadata().getSchema().getClass(Dummy.class.getSimpleName()));

    database.getEntityManager().registerEntityClass(Dummy.class);

    database.countClass(Dummy.class.getSimpleName());

    Assert.assertNotNull(database.getMetadata().getSchema().getClass(Dummy.class.getSimpleName()));

    database.close();
  }

  @Test
  public void testDeletionClassFromSchemaShouldNotLockDatabase() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    Assert.assertNull(database.getMetadata().getSchema().getClass(DummyForTestFreeze.class.getSimpleName()));

    database.getEntityManager().registerEntityClass(DummyForTestFreeze.class);

    database.countClass(Dummy.class.getSimpleName());

    database.getMetadata().getSchema().dropClass(DummyForTestFreeze.class.getSimpleName());

    Assert.assertNotNull(database.getMetadata().getSchema().getClass(DummyForTestFreeze.class.getSimpleName()));

    database.close();
  }

  @Test
  public void testSimpleTypes() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaSimpleTestClass javaObj = database.newInstance(JavaSimpleTestClass.class);
    Assert.assertEquals(javaObj.getText(), "initTest");
    Date date = new Date();
    javaObj.setText("test");
    javaObj.setNumberSimple(12345);
    javaObj.setDoubleSimple(12.34d);
    javaObj.setFloatSimple(123.45f);
    javaObj.setLongSimple(12345678l);
    javaObj.setByteSimple((byte) 1);
    javaObj.setFlagSimple(true);
    javaObj.setDateField(date);
    javaObj.setEnumeration(EnumTest.ENUM1);

    JavaSimpleTestClass savedJavaObj = database.save(javaObj);
    ORID id = database.getIdentity(savedJavaObj);
    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaSimpleTestClass loadedJavaObj = (JavaSimpleTestClass) database.load(id);
    Assert.assertEquals(loadedJavaObj.getText(), "test");
    Assert.assertEquals(loadedJavaObj.getNumberSimple(), 12345);
    Assert.assertEquals(loadedJavaObj.getDoubleSimple(), 12.34d);
    Assert.assertEquals(loadedJavaObj.getFloatSimple(), 123.45f);
    Assert.assertEquals(loadedJavaObj.getLongSimple(), 12345678l);
    Assert.assertEquals(loadedJavaObj.getByteSimple(), (byte) 1);
    Assert.assertEquals(loadedJavaObj.getFlagSimple(), true);
    Assert.assertEquals(loadedJavaObj.getEnumeration(), EnumTest.ENUM1);
    Assert.assertEquals(loadedJavaObj.getDateField(), date);
    Assert.assertTrue(loadedJavaObj.getTestAnonymous() instanceof JavaTestInterface);
    Assert.assertEquals(loadedJavaObj.getTestAnonymous().getNumber(), -1);
    loadedJavaObj.setEnumeration(EnumTest.ENUM2);
    loadedJavaObj.setTestAnonymous(new JavaTestInterface() {

      public int getNumber() {
        return 0;
      }
    });
    database.save(loadedJavaObj);

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loadedJavaObj = (JavaSimpleTestClass) database.load(id);
    Assert.assertEquals(loadedJavaObj.getEnumeration(), EnumTest.ENUM2);
    Assert.assertTrue(loadedJavaObj.getTestAnonymous() instanceof JavaTestInterface);
    Assert.assertEquals(loadedJavaObj.getTestAnonymous().getNumber(), -1);
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testDateInTransaction() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaSimpleTestClass javaObj = new JavaSimpleTestClass();
    Date date = new Date();
    javaObj.setDateField(date);
    database.begin(TXTYPE.OPTIMISTIC);
    JavaSimpleTestClass dbEntry = database.save(javaObj);
    database.commit();
    database.detachAll(dbEntry, false);
    Assert.assertEquals(dbEntry.getDateField(), date);
    // Close db
    database.close();
  }

  @Test(dependsOnMethods = "testAutoCreateClass")
  public void readAndBrowseDescendingAndCheckHoleUtilization() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getLevel1Cache().invalidate();
    database.getLevel2Cache().clear();

    // BROWSE ALL THE OBJECTS

    Set<Integer> ids = new HashSet<Integer>(TOT_RECORDS);
    for (int i = 0; i < TOT_RECORDS; i++)
      ids.add(i);

    for (Account a : database.browseClass(Account.class)) {
      int id = a.getId();
      Assert.assertTrue(ids.remove(id));

      Assert.assertEquals(a.getId(), id);
      Assert.assertEquals(a.getName(), "Bill");
      Assert.assertEquals(a.getSurname(), "Gates");
      Assert.assertEquals(a.getSalary(), id + 300.1f);
      Assert.assertEquals(a.getAddresses().size(), 1);
      Assert.assertEquals(a.getAddresses().get(0).getCity().getName(), rome.getName());
      Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), rome.getCountry().getName());
    }

    Assert.assertTrue(ids.isEmpty());

    database.close();
  }

  @Test(dependsOnMethods = "testAutoCreateClass")
  public void synchQueryCollectionsFetch() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getLevel1Cache().invalidate();
    database.getLevel2Cache().clear();

    // BROWSE ALL THE OBJECTS
    Set<Integer> ids = new HashSet<Integer>(TOT_RECORDS);
    for (int i = 0; i < TOT_RECORDS; i++)
      ids.add(i);

    List<Account> result = database.query(new OSQLSynchQuery<Account>("select from Account").setFetchPlan("*:-1"));
    for (Account a : result) {
      int id = a.getId();
      Assert.assertTrue(ids.remove(id));

      Assert.assertEquals(a.getId(), id);
      Assert.assertEquals(a.getName(), "Bill");
      Assert.assertEquals(a.getSurname(), "Gates");
      Assert.assertEquals(a.getSalary(), id + 300.1f);
      Assert.assertEquals(a.getAddresses().size(), 1);
      Assert.assertEquals(a.getAddresses().get(0).getCity().getName(), rome.getName());
      Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), rome.getCountry().getName());
    }

    Assert.assertTrue(ids.isEmpty());

    database.close();
  }

  @Test(dependsOnMethods = "testAutoCreateClass")
  public void synchQueryCollectionsFetchNoLazyLoad() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getLevel1Cache().invalidate();
    database.getLevel2Cache().clear();
    database.setLazyLoading(false);

    // BROWSE ALL THE OBJECTS
    Set<Integer> ids = new HashSet<Integer>(TOT_RECORDS);
    for (int i = 0; i < TOT_RECORDS; i++)
      ids.add(i);

    List<Account> result = database.query(new OSQLSynchQuery<Account>("select from Account").setFetchPlan("*:2"));
    for (Account a : result) {
      int id = a.getId();
      Assert.assertTrue(ids.remove(id));

      Assert.assertEquals(a.getId(), id);
      Assert.assertEquals(a.getName(), "Bill");
      Assert.assertEquals(a.getSurname(), "Gates");
      Assert.assertEquals(a.getSalary(), id + 300.1f);
      Assert.assertEquals(a.getAddresses().size(), 1);
      Assert.assertEquals(a.getAddresses().get(0).getCity().getName(), rome.getName());
      Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), rome.getCountry().getName());
    }

    Assert.assertTrue(ids.isEmpty());

    database.close();
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void mapEnumAndInternalObjects() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    // BROWSE ALL THE OBJECTS
    for (OUser u : database.browseClass(OUser.class)) {
      u.save();
    }

    database.close();
  }

  @Test(dependsOnMethods = "mapEnumAndInternalObjects")
  public void mapObjectsLinkTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Silvester");

    Child c = database.newInstance(Child.class);
    c.setName("John");

    Child c1 = database.newInstance(Child.class);
    c1.setName("Jack");

    Child c2 = database.newInstance(Child.class);
    c2.setName("Bob");

    Child c3 = database.newInstance(Child.class);
    c3.setName("Sam");

    Child c4 = database.newInstance(Child.class);
    c4.setName("Dean");

    p.getList().add(c1);
    p.getList().add(c2);
    p.getList().add(c3);
    p.getList().add(c4);

    p.getChildren().put("first", c);

    p.getEnumList().add(EnumTest.ENUM1);
    p.getEnumList().add(EnumTest.ENUM2);

    p.getEnumSet().add(EnumTest.ENUM1);
    p.getEnumSet().add(EnumTest.ENUM3);

    p.getEnumMap().put("1", EnumTest.ENUM2);
    p.getEnumMap().put("2", EnumTest.ENUM3);

    database.save(p);

    List<Child> cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    Assert.assertTrue(cresult.size() > 0);

    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    Assert.assertEquals(loaded.getList().size(), 4);
    Assert.assertTrue(loaded.getList().get(0) instanceof Child);
    Assert.assertTrue(loaded.getList().get(1) instanceof Child);
    Assert.assertTrue(loaded.getList().get(2) instanceof Child);
    Assert.assertTrue(loaded.getList().get(3) instanceof Child);
    Assert.assertEquals(loaded.getList().get(0).getName(), "Jack");
    Assert.assertEquals(loaded.getList().get(1).getName(), "Bob");
    Assert.assertEquals(loaded.getList().get(2).getName(), "Sam");
    Assert.assertEquals(loaded.getList().get(3).getName(), "Dean");

    Assert.assertEquals(loaded.getEnumList().size(), 2);
    Assert.assertEquals(loaded.getEnumList().get(0), EnumTest.ENUM1);
    Assert.assertEquals(loaded.getEnumList().get(1), EnumTest.ENUM2);

    Assert.assertEquals(loaded.getEnumSet().size(), 2);
    Iterator<EnumTest> it = loaded.getEnumSet().iterator();
    Assert.assertEquals(it.next(), EnumTest.ENUM1);
    Assert.assertEquals(it.next(), EnumTest.ENUM3);

    Assert.assertEquals(loaded.getEnumMap().size(), 2);
    Assert.assertEquals(loaded.getEnumMap().get("1"), EnumTest.ENUM2);
    Assert.assertEquals(loaded.getEnumMap().get("2"), EnumTest.ENUM3);

    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void listObjectsLinkTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    PersonTest hanSolo = database.newInstance(PersonTest.class);
    hanSolo.setFirstname("Han");
    hanSolo = database.save(hanSolo);

    PersonTest obiWan = database.newInstance(PersonTest.class);
    obiWan.setFirstname("Obi-Wan");
    obiWan = database.save(obiWan);

    PersonTest luke = database.newInstance(PersonTest.class);
    luke.setFirstname("Luke");
    luke = database.save(luke);

    // ============================== step 1
    // add new information to luke
    luke.addFriend(hanSolo);
    database.save(luke);
    Assert.assertTrue(luke.getFriends().size() == 1);
    // ============================== end 1

    // ============================== step 2
    // add new information to luke
    HashSet<PersonTest> friends = new HashSet<PersonTest>();
    friends.add(obiWan);
    luke.setFriends(friends);
    database.save(luke);
    Assert.assertTrue(luke.getFriends().size() == 1);
    // ============================== end 2

    database.close();
  }

  @Test(dependsOnMethods = "listObjectsLinkTest")
  public void mapObjectsListEmbeddedTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    List<Child> cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    int childSize = cresult.size();

    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Silvester");

    Child c = database.newInstance(Child.class);
    c.setName("John");

    Child c1 = database.newInstance(Child.class);
    c1.setName("Jack");

    Child c2 = database.newInstance(Child.class);
    c2.setName("Bob");

    Child c3 = database.newInstance(Child.class);
    c3.setName("Sam");

    Child c4 = database.newInstance(Child.class);
    c4.setName("Dean");

    p.getEmbeddedList().add(c1);
    p.getEmbeddedList().add(c2);
    p.getEmbeddedList().add(c3);
    p.getEmbeddedList().add(c4);

    database.save(p);

    cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    Assert.assertTrue(cresult.size() == childSize);

    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    Assert.assertEquals(loaded.getEmbeddedList().size(), 4);
    Assert.assertTrue(database.getRecordByUserObject(loaded.getEmbeddedList().get(0), false).isEmbedded());
    Assert.assertTrue(database.getRecordByUserObject(loaded.getEmbeddedList().get(1), false).isEmbedded());
    Assert.assertTrue(database.getRecordByUserObject(loaded.getEmbeddedList().get(2), false).isEmbedded());
    Assert.assertTrue(database.getRecordByUserObject(loaded.getEmbeddedList().get(3), false).isEmbedded());
    Assert.assertTrue(loaded.getEmbeddedList().get(0) instanceof Child);
    Assert.assertTrue(loaded.getEmbeddedList().get(1) instanceof Child);
    Assert.assertTrue(loaded.getEmbeddedList().get(2) instanceof Child);
    Assert.assertTrue(loaded.getEmbeddedList().get(3) instanceof Child);
    Assert.assertEquals(loaded.getEmbeddedList().get(0).getName(), "Jack");
    Assert.assertEquals(loaded.getEmbeddedList().get(1).getName(), "Bob");
    Assert.assertEquals(loaded.getEmbeddedList().get(2).getName(), "Sam");
    Assert.assertEquals(loaded.getEmbeddedList().get(3).getName(), "Dean");

    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsListEmbeddedTest")
  public void mapObjectsSetEmbeddedTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    List<Child> cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    int childSize = cresult.size();

    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Silvester");

    Child c = database.newInstance(Child.class);
    c.setName("John");

    Child c1 = database.newInstance(Child.class);
    c1.setName("Jack");

    Child c2 = database.newInstance(Child.class);
    c2.setName("Bob");

    Child c3 = database.newInstance(Child.class);
    c3.setName("Sam");

    Child c4 = database.newInstance(Child.class);
    c4.setName("Dean");

    p.getEmbeddedSet().add(c);
    p.getEmbeddedSet().add(c1);
    p.getEmbeddedSet().add(c2);
    p.getEmbeddedSet().add(c3);
    p.getEmbeddedSet().add(c4);

    database.save(p);

    cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    Assert.assertTrue(cresult.size() == childSize);

    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    Assert.assertEquals(loaded.getEmbeddedSet().size(), 5);
    Iterator<Child> it = loaded.getEmbeddedSet().iterator();
    while (it.hasNext()) {
      Child loadedC = it.next();
      Assert.assertTrue(database.getRecordByUserObject(loadedC, false).isEmbedded());
      Assert.assertTrue(loadedC instanceof Child);
      Assert.assertTrue(loadedC.getName().equals("John") || loadedC.getName().equals("Jack") || loadedC.getName().equals("Bob")
          || loadedC.getName().equals("Sam") || loadedC.getName().equals("Dean"));
    }

    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsSetEmbeddedTest")
  public void mapObjectsMapEmbeddedTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    List<Child> cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    int childSize = cresult.size();

    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Silvester");

    Child c = database.newInstance(Child.class);
    c.setName("John");

    Child c1 = database.newInstance(Child.class);
    c1.setName("Jack");

    Child c2 = database.newInstance(Child.class);
    c2.setName("Bob");

    Child c3 = database.newInstance(Child.class);
    c3.setName("Sam");

    Child c4 = database.newInstance(Child.class);
    c4.setName("Dean");

    p.getEmbeddedChildren().put(c.getName(), c);
    p.getEmbeddedChildren().put(c1.getName(), c1);
    p.getEmbeddedChildren().put(c2.getName(), c2);
    p.getEmbeddedChildren().put(c3.getName(), c3);
    p.getEmbeddedChildren().put(c4.getName(), c4);

    database.save(p);

    cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    Assert.assertTrue(cresult.size() == childSize);

    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    Assert.assertEquals(loaded.getEmbeddedChildren().size(), 5);
    for (String key : loaded.getEmbeddedChildren().keySet()) {
      Child loadedC = loaded.getEmbeddedChildren().get(key);
      Assert.assertTrue(database.getRecordByUserObject(loadedC, false).isEmbedded());
      Assert.assertTrue(loadedC instanceof Child);
      Assert.assertTrue(loadedC.getName().equals("John") || loadedC.getName().equals("Jack") || loadedC.getName().equals("Bob")
          || loadedC.getName().equals("Sam") || loadedC.getName().equals("Dean"));
    }

    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsNonExistingKeyTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    JavaComplexTestClass p = new JavaComplexTestClass();
    p.setName("Silvester");

    p = database.save(p);

    Child c1 = new Child();
    c1.setName("John");

    Child c2 = new Child();
    c2.setName("Jack");

    p.getChildren().put("first", c1);
    p.getChildren().put("second", c2);

    database.save(p);

    Child c3 = new Child();
    c3.setName("Olivia");
    Child c4 = new Child();
    c4.setName("Peter");

    p.getChildren().put("third", c3);
    p.getChildren().put("fourth", c4);

    database.save(p);

    List<Child> cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    Assert.assertTrue(cresult.size() > 0);

    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    Assert.assertEquals(loaded.getChildren().get("first").getName(), c1.getName());
    Assert.assertEquals(loaded.getChildren().get("second").getName(), c2.getName());
    Assert.assertEquals(loaded.getChildren().get("third").getName(), c3.getName());
    Assert.assertEquals(loaded.getChildren().get("fourth").getName(), c4.getName());
    Assert.assertEquals(loaded.getChildren().get("fifth"), null);

    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkTwoSaveTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    JavaComplexTestClass p = new JavaComplexTestClass();
    p.setName("Silvester");

    p = database.save(p);

    Child c1 = new Child();
    c1.setName("John");

    Child c2 = new Child();
    c2.setName("Jack");

    p.getChildren().put("first", c1);
    p.getChildren().put("second", c2);

    database.save(p);

    Child c3 = new Child();
    c3.setName("Olivia");
    Child c4 = new Child();
    c4.setName("Peter");

    p.getChildren().put("third", c3);
    p.getChildren().put("fourth", c4);

    database.save(p);

    List<Child> cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

    Assert.assertTrue(cresult.size() > 0);

    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    Assert.assertEquals(loaded.getChildren().get("first").getName(), c1.getName());
    Assert.assertEquals(loaded.getChildren().get("second").getName(), c2.getName());
    Assert.assertEquals(loaded.getChildren().get("third").getName(), c3.getName());
    Assert.assertEquals(loaded.getChildren().get("fourth").getName(), c4.getName());

    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void enumQueryTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    JavaComplexTestClass testEnum1 = database.newInstance(JavaComplexTestClass.class);
    testEnum1.setName("Silvester");

    JavaComplexTestClass testEnum2 = database.newInstance(JavaComplexTestClass.class);
    testEnum1.setName("Silvester");

    testEnum1.setEnumField(EnumTest.ENUM1);

    testEnum1.getEnumList().add(EnumTest.ENUM1);
    testEnum1.getEnumList().add(EnumTest.ENUM2);

    testEnum1.getEnumSet().add(EnumTest.ENUM1);
    testEnum1.getEnumSet().add(EnumTest.ENUM3);

    testEnum1.getEnumMap().put("1", EnumTest.ENUM2);
    testEnum1.getEnumMap().put("2", EnumTest.ENUM3);

    testEnum2.setEnumField(EnumTest.ENUM2);

    testEnum2.getEnumList().add(EnumTest.ENUM2);
    testEnum2.getEnumList().add(EnumTest.ENUM3);

    testEnum2.getEnumSet().add(EnumTest.ENUM1);
    testEnum2.getEnumSet().add(EnumTest.ENUM2);

    database.save(testEnum1);
    database.save(testEnum2);

    ORID enum1Rid = database.getIdentity(testEnum1);
    ORID enum2Rid = database.getIdentity(testEnum2);

    OSQLSynchQuery<JavaComplexTestClass> enumFieldQuery = new OSQLSynchQuery<JavaComplexTestClass>(
        "select from JavaComplexTestClass where enumField = :enumField");

    Map<String, Object> enum1Config = new HashMap<String, Object>();
    Map<String, Object> enum2Config = new HashMap<String, Object>();
    enum1Config.put("enumField", EnumTest.ENUM1);
    enum2Config.put("enumField", EnumTest.ENUM2);
    List<JavaComplexTestClass> result = database.query(enumFieldQuery, enum1Config);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(database.getIdentity(result.get(0)).getIdentity(), enum1Rid);

    result = database.query(enumFieldQuery, enum2Config);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(database.getIdentity(result.get(0)).getIdentity(), enum2Rid);

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    result = database.query(enumFieldQuery, enum1Config);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(database.getIdentity(result.get(0)).getIdentity(), enum1Rid);

    result = database.query(enumFieldQuery, enum2Config);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(database.getIdentity(result.get(0)).getIdentity(), enum2Rid);

    database.close();
  }

  @Test(dependsOnMethods = "enumQueryTest")
  public void paramQueryTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    JavaComplexTestClass testObject = database.newInstance(JavaComplexTestClass.class);
    testObject.setName("Silvester");
    Child child = database.newInstance(Child.class);
    testObject.setChild(child);
    testObject.setEnumField(EnumTest.ENUM1);

    database.save(testObject);

    ORID testObjectRid = database.getIdentity(testObject);
    ORID childRid = database.getIdentity(child);

    OSQLSynchQuery<JavaComplexTestClass> enumFieldQuery = new OSQLSynchQuery<JavaComplexTestClass>(
        "select from JavaComplexTestClass where enumField = :enumField and child = :child");

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("child", childRid);
    params.put("enumField", EnumTest.ENUM1);
    List<JavaComplexTestClass> result = database.query(enumFieldQuery, params);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(database.getIdentity(result.get(0)).getIdentity(), testObjectRid);

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    enumFieldQuery = new OSQLSynchQuery<JavaComplexTestClass>(
        "select from JavaComplexTestClass where enumField = :enumField and child = :child");
    result = database.query(enumFieldQuery, params);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(database.getIdentity(result.get(0)).getIdentity(), testObjectRid);

    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkUpdateDatabaseNewInstanceTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    // TEST WITH NEW INSTANCE
    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Fringe");

    Child c = database.newInstance(Child.class);
    c.setName("Peter");
    Child c1 = database.newInstance(Child.class);
    c1.setName("Walter");
    Child c2 = database.newInstance(Child.class);
    c2.setName("Olivia");
    Child c3 = database.newInstance(Child.class);
    c3.setName("Astrid");

    p.getChildren().put(c.getName(), c);
    p.getChildren().put(c1.getName(), c1);
    p.getChildren().put(c2.getName(), c2);
    p.getChildren().put(c3.getName(), c3);

    // database.begin();
    database.save(p);
    // database.commit();
    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    for (String key : loaded.getChildren().keySet()) {
      Assert.assertTrue(key.equals("Peter") || key.equals("Walter") || key.equals("Olivia") || key.equals("Astrid"));
      Assert.assertTrue(loaded.getChildren().get(key) instanceof Child);
      Assert.assertTrue(loaded.getChildren().get(key).getName().equals(key));
      if (key.equals("Peter")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Peter"));
      } else if (key.equals("Walter")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Walter"));
      } else if (key.equals("Olivia")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Olivia"));
      } else if (key.equals("Astrid")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Astrid"));
      }
    }

    database.setLazyLoading(false);
    for (JavaComplexTestClass reloaded : database.browseClass(JavaComplexTestClass.class).setFetchPlan("*:-1")) {
      database.reload(reloaded);

      Child c4 = database.newInstance(Child.class);
      c4.setName("The Observer");

      reloaded.getChildren().put(c4.getName(), c4);
      database.save(reloaded);
    }
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    for (JavaComplexTestClass reloaded : database.browseClass(JavaComplexTestClass.class).setFetchPlan("*:-1")) {
      database.reload(reloaded);
      Assert.assertTrue(reloaded.getChildren().containsKey("The Observer"));
      Assert.assertTrue(reloaded.getChildren().get("The Observer") != null);
      Assert.assertEquals(reloaded.getChildren().get("The Observer").getName(), "The Observer");
      Assert.assertTrue(database.getIdentity(reloaded.getChildren().get("The Observer")).isPersistent()
          && database.getIdentity(reloaded.getChildren().get("The Observer")).isValid());
    }
    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateDatabaseNewInstanceTest")
  public void mapObjectsLinkUpdateJavaNewInstanceTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    // TEST WITH NEW INSTANCE
    JavaComplexTestClass p = new JavaComplexTestClass();
    p.setName("Fringe");

    Child c = new Child();
    c.setName("Peter");
    Child c1 = new Child();
    c1.setName("Walter");
    Child c2 = new Child();
    c2.setName("Olivia");
    Child c3 = new Child();
    c3.setName("Astrid");

    p.getChildren().put(c.getName(), c);
    p.getChildren().put(c1.getName(), c1);
    p.getChildren().put(c2.getName(), c2);
    p.getChildren().put(c3.getName(), c3);

    p = database.save(p);
    ORID rid = new ORecordId(p.getId());

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);

    for (String key : loaded.getChildren().keySet()) {
      Assert.assertTrue(key.equals("Peter") || key.equals("Walter") || key.equals("Olivia") || key.equals("Astrid"));
      Assert.assertTrue(loaded.getChildren().get(key) instanceof Child);
      Assert.assertTrue(loaded.getChildren().get(key).getName().equals(key));
      if (key.equals("Peter")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Peter"));
      } else if (key.equals("Walter")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Walter"));
      } else if (key.equals("Olivia")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Olivia"));
      } else if (key.equals("Astrid")) {
        Assert.assertTrue(loaded.getChildren().get(key).getName().equals("Astrid"));
      }
    }

    database.setLazyLoading(false);
    for (JavaComplexTestClass reloaded : database.browseClass(JavaComplexTestClass.class).setFetchPlan("*:-1")) {
      database.reload(reloaded);

      Child c4 = new Child();
      c4.setName("The Observer");

      reloaded.getChildren().put(c4.getName(), c4);
      database.save(reloaded);
    }
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    for (JavaComplexTestClass reloaded : database.browseClass(JavaComplexTestClass.class).setFetchPlan("*:-1")) {
      database.reload(reloaded);
      Assert.assertTrue(reloaded.getChildren().containsKey("The Observer"));
      Assert.assertTrue(reloaded.getChildren().get("The Observer") != null);
      Assert.assertEquals(reloaded.getChildren().get("The Observer").getName(), "The Observer");
      Assert.assertTrue(database.getIdentity(reloaded.getChildren().get("The Observer")).isPersistent()
          && database.getIdentity(reloaded.getChildren().get("The Observer")).isValid());
    }
    database.close();
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateJavaNewInstanceTest")
  public void mapStringTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Map<String, String> relatives = new HashMap<String, String>();
    relatives.put("father", "Mike");
    relatives.put("mother", "Julia");

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND HANDLER MANAGEMENT
    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Chuck");
    p.getStringMap().put("father", "Mike");
    p.getStringMap().put("mother", "Julia");

    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), p.getStringMap().get(referenceRelativ));
    }

    database.save(p);
    ORID rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringMap());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    loaded.getStringMap().put("brother", "Nike");
    relatives.put("brother", "Nike");
    database.save(loaded);
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringMap());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    database.delete(loaded);

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Chuck");
    p.setStringMap(relatives);

    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), p.getStringMap().get(referenceRelativ));
    }

    database.save(p);
    rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringMap());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    loaded.getStringMap().put("brother", "Nike");
    relatives.put("brother", "Nike");
    database.save(loaded);
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringMap());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    database.delete(loaded);

    // TEST WITH JAVA CONSTRUCTOR
    p = new JavaComplexTestClass();
    p.setName("Chuck");
    p.setStringMap(relatives);

    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), p.getStringMap().get(referenceRelativ));
    }

    p = database.save(p);
    rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringMap());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    loaded.getStringMap().put("brother", "Nike");
    relatives.put("brother", "Nike");
    database.save(loaded);
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringMap());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getStringMap().get(referenceRelativ));
    }
    database.delete(loaded);
    database.close();
  }

  @Test(dependsOnMethods = "mapStringTest")
  public void setStringTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass testClass = new JavaComplexTestClass();
    Set<String> roles = new HashSet<String>();
    roles.add("manager");
    roles.add("developer");
    testClass.setStringSet(roles);

    JavaComplexTestClass testClassProxy = database.save(testClass);
    Assert.assertEquals(roles.size(), testClassProxy.getStringSet().size());
    for (String referenceRole : roles) {
      Assert.assertTrue(testClassProxy.getStringSet().contains(referenceRole));
    }

    ORID orid = database.getIdentity(testClassProxy);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    JavaComplexTestClass loadedProxy = database.load(orid);
    Assert.assertEquals(roles.size(), loadedProxy.getStringSet().size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.getStringSet().contains(referenceRole));
    }

    database.save(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.getStringSet().size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.getStringSet().contains(referenceRole));
    }

    loadedProxy.getStringSet().remove("developer");
    roles.remove("developer");
    database.save(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.getStringSet().size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.getStringSet().contains(referenceRole));
    }
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    loadedProxy = database.load(orid);
    Assert.assertEquals(roles.size(), loadedProxy.getStringSet().size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.getStringSet().contains(referenceRole));
    }
    database.close();
  }

  @Test(dependsOnMethods = "setStringTest")
  public void mapStringListTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Map<String, List<String>> songAndMovies = new HashMap<String, List<String>>();
    List<String> movies = new ArrayList<String>();
    List<String> songs = new ArrayList<String>();
    movies.add("Star Wars");
    movies.add("Star Wars: The Empire Strikes Back");
    movies.add("Star Wars: The return of the Jedi");
    songs.add("Metallica - Master of Puppets");
    songs.add("Daft Punk - Harder, Better, Faster, Stronger");
    songs.add("Johnny Cash - Cocaine Blues");
    songs.add("Skrillex - Scary Monsters & Nice Sprites");
    songAndMovies.put("movies", movies);
    songAndMovies.put("songs", songs);

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND HANDLER MANAGEMENT
    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Chuck");

    p.getStringListMap().put("movies", movies);
    p.getStringListMap().put("songs", songs);

    for (String referenceRelativ : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(referenceRelativ), p.getStringListMap().get(referenceRelativ));
    }

    database.save(p);
    ORID rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringListMap());
    for (String reference : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(reference), loaded.getStringListMap().get(reference));
    }
    database.delete(loaded);

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Chuck");
    p.setStringListMap(songAndMovies);

    for (String referenceRelativ : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(referenceRelativ), p.getStringListMap().get(referenceRelativ));
    }

    database.save(p);
    rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringListMap());
    for (String referenceRelativ : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(referenceRelativ), loaded.getStringListMap().get(referenceRelativ));
    }
    database.delete(loaded);

    // TEST WITH OBJECT DATABASE NEW INSTANCE LIST DIRECT ADD
    p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Chuck");
    p.getStringListMap().put("songs", new ArrayList<String>());
    p.getStringListMap().get("songs").add("Metallica - Master of Puppets");
    p.getStringListMap().get("songs").add("Daft Punk - Harder, Better, Faster, Stronger");
    p.getStringListMap().get("songs").add("Johnny Cash - Cocaine Blues");
    p.getStringListMap().get("songs").add("Skrillex - Scary Monsters & Nice Sprites");
    p.getStringListMap().put("movies", new ArrayList<String>());
    p.getStringListMap().get("movies").add("Star Wars");
    p.getStringListMap().get("movies").add("Star Wars: The Empire Strikes Back");
    p.getStringListMap().get("movies").add("Star Wars: The return of the Jedi");

    for (String referenceRelativ : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(referenceRelativ), p.getStringListMap().get(referenceRelativ));
    }

    database.save(p);
    rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringListMap());
    for (String referenceRelativ : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(referenceRelativ), loaded.getStringListMap().get(referenceRelativ));
    }
    database.delete(loaded);

    // TEST WITH JAVA CONSTRUCTOR
    p = new JavaComplexTestClass();
    p.setName("Chuck");
    p.setStringListMap(songAndMovies);

    for (String referenceRelativ : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(referenceRelativ), p.getStringListMap().get(referenceRelativ));
    }

    p = database.save(p);
    rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getStringListMap());
    for (String referenceRelativ : songAndMovies.keySet()) {
      Assert.assertEquals(songAndMovies.get(referenceRelativ), loaded.getStringListMap().get(referenceRelativ));
    }
    database.delete(loaded);
    database.close();
  }

  @Test(dependsOnMethods = "mapStringListTest")
  public void mapStringObjectTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Map<String, Object> relatives = new HashMap<String, Object>();
    relatives.put("father", "Mike");
    relatives.put("mother", "Julia");

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND HANDLER MANAGEMENT
    JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Chuck");
    p.getMapObject().put("father", "Mike");
    p.getMapObject().put("mother", "Julia");

    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), p.getMapObject().get(referenceRelativ));
    }

    p.getMapObject().keySet().size();

    database.save(p);
    ORID rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    JavaComplexTestClass loaded = database.load(rid);
    Assert.assertNotNull(loaded.getMapObject());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    loaded.getMapObject().keySet().size();
    loaded.getMapObject().put("brother", "Nike");
    relatives.put("brother", "Nike");
    database.save(loaded);
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    loaded.getMapObject().keySet().size();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getMapObject());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    database.delete(loaded);

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    p = database.newInstance(JavaComplexTestClass.class);
    p.setName("Chuck");
    p.setMapObject(relatives);

    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), p.getMapObject().get(referenceRelativ));
    }

    database.save(p);
    p.getMapObject().keySet().size();
    rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getMapObject());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    loaded.getMapObject().keySet().size();
    loaded.getMapObject().put("brother", "Nike");
    relatives.put("brother", "Nike");
    database.save(loaded);
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    loaded.getMapObject().keySet().size();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getMapObject());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    loaded.getMapObject().keySet().size();
    database.delete(loaded);

    // TEST WITH JAVA CONSTRUCTOR
    p = new JavaComplexTestClass();
    p.setName("Chuck");
    p.setMapObject(relatives);

    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), p.getMapObject().get(referenceRelativ));
    }

    p = database.save(p);
    p.getMapObject().keySet().size();
    rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.getMapObject());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    loaded.getMapObject().keySet().size();
    loaded.getMapObject().put("brother", "Nike");
    relatives.put("brother", "Nike");
    database.save(loaded);
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    loaded = database.load(rid);
    loaded.getMapObject().keySet().size();
    Assert.assertNotNull(loaded.getMapObject());
    for (String referenceRelativ : relatives.keySet()) {
      Assert.assertEquals(relatives.get(referenceRelativ), loaded.getMapObject().get(referenceRelativ));
    }
    database.delete(loaded);
    database.close();
  }

  @Test(dependsOnMethods = "mapStringObjectTest")
  public void oidentifableFieldsTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
      p.setName("Dean Winchester");

      ODocument testEmbeddedDocument = new ODocument();
      testEmbeddedDocument.field("testEmbeddedField", "testEmbeddedValue");

      p.setEmbeddedDocument(testEmbeddedDocument);

      ODocument testDocument = new ODocument();
      testDocument.field("testField", "testValue");

      p.setDocument(testDocument);

      ORecordBytes testRecordBytes = new ORecordBytes(
          "this is a bytearray test. if you read this Object database has stored it correctly".getBytes());

      p.setByteArray(testRecordBytes);

      database.save(p);

      ORID rid = database.getRecordByUserObject(p, false).getIdentity();

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      JavaComplexTestClass loaded = database.load(rid);

      Assert.assertTrue(loaded.getByteArray() instanceof ORecordBytes);
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          loaded.getByteArray().toOutputStream(out);
          Assert.assertEquals("this is a bytearray test. if you read this Object database has stored it correctly".getBytes(),
              out.toByteArray());
          Assert.assertEquals("this is a bytearray test. if you read this Object database has stored it correctly",
              new String(out.toByteArray()));
        } finally {
          out.close();
        }
      } catch (IOException ioe) {
        Assert.assertTrue(false);
        OLogManager.instance().error(this, "Error reading byte[]", ioe);
      }
      Assert.assertTrue(loaded.getDocument() instanceof ODocument);
      Assert.assertEquals("testValue", loaded.getDocument().field("testField"));
      Assert.assertTrue(loaded.getDocument().getIdentity().isPersistent());

      Assert.assertTrue(loaded.getEmbeddedDocument() instanceof ODocument);
      Assert.assertEquals("testEmbeddedValue", loaded.getEmbeddedDocument().field("testEmbeddedField"));
      Assert.assertFalse(loaded.getEmbeddedDocument().getIdentity().isValid());

      database.close();
      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      p = database.newInstance(JavaComplexTestClass.class);
      byte[] thumbnailImageBytes = "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
          .getBytes();
      ORecordBytes oRecordBytes = new ORecordBytes(database.getUnderlying(), thumbnailImageBytes);
      oRecordBytes.save();
      p.setByteArray(oRecordBytes);
      p = database.save(p);
      Assert.assertTrue(p.getByteArray() instanceof ORecordBytes);
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          p.getByteArray().toOutputStream(out);
          Assert.assertEquals(
              "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2".getBytes(),
              out.toByteArray());
          Assert.assertEquals("this is a bytearray test. if you read this Object database has stored it correctlyVERSION2",
              new String(out.toByteArray()));
        } finally {
          out.close();
        }
      } catch (IOException ioe) {
        Assert.assertTrue(false);
        OLogManager.instance().error(this, "Error reading byte[]", ioe);
      }
      rid = database.getIdentity(p);

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      loaded = database.load(rid);

      Assert.assertTrue(loaded.getByteArray() instanceof ORecordBytes);
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          loaded.getByteArray().toOutputStream(out);
          Assert.assertEquals(
              "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2".getBytes(),
              out.toByteArray());
          Assert.assertEquals("this is a bytearray test. if you read this Object database has stored it correctlyVERSION2",
              new String(out.toByteArray()));
        } finally {
          out.close();
        }
      } catch (IOException ioe) {
        Assert.assertTrue(false);
        OLogManager.instance().error(this, "Error reading byte[]", ioe);
      }
      database.close();
      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      p = new JavaComplexTestClass();
      thumbnailImageBytes = "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2".getBytes();
      oRecordBytes = new ORecordBytes(database.getUnderlying(), thumbnailImageBytes);
      oRecordBytes.save();
      p.setByteArray(oRecordBytes);
      p = database.save(p);
      Assert.assertTrue(p.getByteArray() instanceof ORecordBytes);
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          p.getByteArray().toOutputStream(out);
          Assert.assertEquals(
              "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2".getBytes(),
              out.toByteArray());
          Assert.assertEquals("this is a bytearray test. if you read this Object database has stored it correctlyVERSION2",
              new String(out.toByteArray()));
        } finally {
          out.close();
        }
      } catch (IOException ioe) {
        Assert.assertTrue(false);
        OLogManager.instance().error(this, "Error reading byte[]", ioe);
      }
      rid = database.getIdentity(p);

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      loaded = database.load(rid);

      Assert.assertTrue(loaded.getByteArray() instanceof ORecordBytes);
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          loaded.getByteArray().toOutputStream(out);
          Assert.assertEquals(
              "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2".getBytes(),
              out.toByteArray());
          Assert.assertEquals("this is a bytearray test. if you read this Object database has stored it correctlyVERSION2",
              new String(out.toByteArray()));
        } finally {
          out.close();
        }
      } catch (IOException ioe) {
        Assert.assertTrue(false);
        OLogManager.instance().error(this, "Error reading byte[]", ioe);
      }
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "oidentifableFieldsTest")
  public void oRecordBytesFieldsTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      OObjectIteratorClass<JavaComplexTestClass> browseClass = database.browseClass(JavaComplexTestClass.class);
      for (JavaComplexTestClass ebookPropertyItem : browseClass) {
        ORecordBytes coverThumbnail = ebookPropertyItem.getByteArray(); // The IllegalArgumentException is thrown here.
      }
    } catch (IllegalArgumentException iae) {
      Assert.fail("ORecordBytes field getter should not throw this exception", iae);
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapEnumAndInternalObjects")
  public void afterDeserializationCall() {
    // COMMENTED SINCE SERIALIZATION AND DESERIALIZATION

    // database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    // // TODO TO DELETE WHEN IMPLEMENTED STATIC ENTITY MANAGER
    // database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
    //
    // // BROWSE ALL THE OBJECTS
    // for (Account a : database.browseClass(Account.class)) {
    // Assert.assertTrue(a.isInitialized());
    // }
    // database.close();
  }

  @Test(dependsOnMethods = "afterDeserializationCall")
  public void update() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    int i = 0;
    Account a;
    for (Object o : database.browseCluster("Account").setFetchPlan("*:1")) {
      a = (Account) o;

      if (i % 2 == 0)
        a.getAddresses().set(0, new Address("work", new City(new Country("Spain"), "Madrid"), "Plaza central"));

      a.setSalary(i + 500.10f);

      database.save(a);

      i++;
    }

    database.close();
  }

  @Test(dependsOnMethods = "update")
  public void testUpdate() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    int i = 0;
    Account a;
    for (OObjectIteratorCluster<Account> iterator = database.browseCluster("Account"); iterator.hasNext();) {
      iterator.setFetchPlan("*:1");
      a = iterator.next();

      if (i % 2 == 0)
        Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), "Spain");
      else
        Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), "Italy");

      Assert.assertEquals(a.getSalary(), i + 500.1f);

      i++;
    }

    database.close();
  }

  @Test(dependsOnMethods = "testUpdate")
  public void createLinked() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    long profiles = database.countClass("Profile");

    Profile neo = new Profile("Neo").setValue("test").setLocation(
        new Address("residence", new City(new Country("Spain"), "Madrid"), "Rio de Castilla"));
    neo.addFollowing(new Profile("Morpheus"));
    neo.addFollowing(new Profile("Trinity"));

    database.save(neo);

    Assert.assertEquals(database.countClass("Profile"), profiles + 3);

    database.close();
  }

  @Test(dependsOnMethods = "createLinked")
  public void browseLinked() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    for (Profile obj : database.browseClass(Profile.class).setFetchPlan("*:1")) {
      if (obj.getNick().equals("Neo")) {
        Assert.assertEquals(obj.getFollowers().size(), 0);
        Assert.assertEquals(obj.getFollowings().size(), 2);
      } else if (obj.getNick().equals("Morpheus") || obj.getNick().equals("Trinity")) {
        Assert.assertEquals(obj.getFollowers().size(), 1);
        Assert.assertEquals(obj.getFollowings().size(), 0);
      }
    }

    database.close();
  }

  @Test(dependsOnMethods = "createLinked")
  public void checkLazyLoadingOff() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.setLazyLoading(false);
    for (Profile obj : database.browseClass(Profile.class).setFetchPlan("*:1")) {
      Assert.assertTrue(!(obj.getFollowings() instanceof OLazyObjectSetInterface)
          || ((OLazyObjectSetInterface<Profile>) obj.getFollowings()).isConverted());
      Assert.assertTrue(!(obj.getFollowers() instanceof OLazyObjectSetInterface)
          || ((OLazyObjectSetInterface<Profile>) obj.getFollowers()).isConverted());
      if (obj.getNick().equals("Neo")) {
        Assert.assertEquals(obj.getFollowers().size(), 0);
        Assert.assertEquals(obj.getFollowings().size(), 2);
      } else if (obj.getNick().equals("Morpheus") || obj.getNick().equals("Trinity")) {
        Assert.assertEquals(obj.getFollowings().size(), 0);
        Assert.assertEquals(obj.getFollowers().size(), 1);
        Assert.assertTrue(obj.getFollowers().iterator().next() instanceof Profile);
      }
    }

    database.close();
  }

  @Test(dependsOnMethods = "checkLazyLoadingOff")
  public void queryPerFloat() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    final List<Account> result = database.query(new OSQLSynchQuery<ODocument>("select * from Account where salary = 500.10"));

    Assert.assertTrue(result.size() > 0);

    Account account;
    for (int i = 0; i < result.size(); ++i) {
      account = result.get(i);

      Assert.assertEquals(account.getSalary(), 500.10f);
    }

    database.close();
  }

  @Test(dependsOnMethods = "queryPerFloat")
  public void queryCross3Levels() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final List<Profile> result = database.query(new OSQLSynchQuery<Profile>(
        "select from Profile where location.city.country.name = 'Spain'"));

    Assert.assertTrue(result.size() > 0);

    Profile profile;
    for (int i = 0; i < result.size(); ++i) {
      profile = result.get(i);

      Assert.assertEquals(profile.getLocation().getCity().getCountry().getName(), "Spain");
    }

    database.close();
  }

  @Test(dependsOnMethods = "queryCross3Levels")
  public void deleteFirst() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    startRecordNumber = database.countClusterElements("Account");

    // DELETE ALL THE RECORD IN THE CLUSTER
    for (Object obj : database.browseCluster("Account")) {
      database.delete(obj);
      break;
    }

    Assert.assertEquals(database.countClusterElements("Account"), startRecordNumber - 1);

    database.close();
  }

  @Test
  public void commandWithPositionalParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
    List<Profile> result = database.command(query).execute("Barack", "Obama");

    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void queryWithPositionalParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
    List<Profile> result = database.query(query, "Barack", "Obama");

    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void queryWithRidAsParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    Profile profile = (Profile) database.browseClass("Profile").next();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
    List<Profile> result = database.query(query, new ORecordId(profile.getId()));

    Assert.assertEquals(result.size(), 1);

    database.close();
  }

  @Test
  public void queryWithRidStringAsParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    Profile profile = (Profile) database.browseClass("Profile").next();

    OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
    List<Profile> result = database.query(query, profile.getId());

    Assert.assertEquals(result.size(), 1);

    // TEST WITHOUT # AS PREFIX
    query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
    result = database.query(query, profile.getId().substring(1));

    Assert.assertEquals(result.size(), 1);

    database.close();
  }

  @Test
  public void commandWithNamedParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
        "select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    List<Profile> result = database.command(query).execute(params);
    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test(expectedExceptions = OQueryParsingException.class)
  public void commandWithWrongNamedParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
        "select from Profile where name = :name and surname = :surname%");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    List<Profile> result = database.command(query).execute(params);
    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void queryWithNamedParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
        "select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    List<Profile> result = database.query(query, params);
    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void queryWithObjectAsParameter() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
        "select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    List<Profile> result = database.query(query, params);
    Assert.assertTrue(result.size() != 0);

    Profile obama = result.get(0);

    result = database.query(new OSQLSynchQuery<Profile>("select from Profile where followings contains ( @Rid = :who )"), obama);
    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void queryWithListOfObjectAsParameter() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
        "select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    List<Profile> result = database.query(query, params);
    Assert.assertTrue(result.size() != 0);

    result = database.query(new OSQLSynchQuery<Profile>("select from Profile where followings in (:who)"), result);
    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void queryConcatAttrib() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    Assert.assertTrue(database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country'")).size() > 0);
    Assert.assertEquals(database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country22'")).size(),
        0);

    database.close();
  }

  @Test
  public void queryPreparredTwice() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
        "select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    List<Profile> result = database.query(query, params);
    Assert.assertTrue(result.size() != 0);

    result = database.query(query, params);
    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @Test
  public void commandPreparredTwice() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
        "select from Profile where name = :name and surname = :surname");

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    List<Profile> result = database.command(query).execute(params);
    Assert.assertTrue(result.size() != 0);

    result = database.command(query).execute(params);
    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @SuppressWarnings("deprecation")
  public void testOldObjectImplementation() {
    ODatabaseObjectTx db = new ODatabaseObjectTx(url).open("admin", "admin");
    db.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    db.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    db.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");
    // insert some instruments
    Instrument instr = new Instrument("Fender Stratocaster");
    db.save(instr);
    Instrument instr2 = new Instrument("Music Man");
    db.save(instr2);
    // Insert some musicians
    Musician man = new Musician();
    man.setName("Jack");
    OObjectIteratorClass<Object> list = db.browseClass("Instrument");
    for (Object anInstrument : list) {
      man.getInstruments().add((Instrument) anInstrument);
    }
    db.save(man);
    Musician man2 = new Musician();
    man2.setName("Roger");
    String query = "select from Instrument where name like 'Fender%'";
    List<IdObject> list2 = db.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertTrue(!(list2.get(0) instanceof Proxy));
    man2.getInstruments().add((Instrument) list2.get(0));
    db.save(man2);
    //
    db.close();
    db = new ODatabaseObjectTx(url).open("admin", "admin");
    db.getEntityManager().registerEntityClasses("com.e_soa.dbobjects");
    query = "select from Musician limit 1";
    List<IdObject> list3 = db.query(new OSQLSynchQuery<ODocument>(query));
    man = (Musician) list3.get(0);
    Assert.assertTrue(!(man instanceof Proxy));
    for (Object aObject : man.getInstruments()) {
      Assert.assertTrue(!(aObject instanceof Proxy));
    }
    db.close();
    db = new ODatabaseObjectTx(url).open("admin", "admin");
    list3 = db.query(new OSQLSynchQuery<ODocument>(query));
    man = (Musician) list3.get(0);
    man.setName("Big Jack");
    db.save(man); // here is the exception
    db.close();
  }

  @Test(dependsOnMethods = "oidentifableFieldsTest")
  public void testEmbeddedDeletion() throws Exception {
    OObjectDatabaseTx db = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    Parent parent = db.newInstance(Parent.class);
    parent.setName("Big Parent");

    EmbeddedChild embedded = db.newInstance(EmbeddedChild.class);
    embedded.setName("Little Child");

    parent.setEmbeddedChild(embedded);

    parent = db.save(parent);

    List<Parent> presult = db.query(new OSQLSynchQuery<Parent>("select from Parent"));
    List<EmbeddedChild> cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select from EmbeddedChild"));
    Assert.assertEquals(presult.size(), 1);
    Assert.assertEquals(cresult.size(), 0);

    EmbeddedChild child = db.newInstance(EmbeddedChild.class);
    child.setName("Little Child");
    parent.setChild(child);

    parent = db.save(parent);

    presult = db.query(new OSQLSynchQuery<Parent>("select from Parent"));
    cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select from EmbeddedChild"));
    Assert.assertEquals(presult.size(), 1);
    Assert.assertEquals(cresult.size(), 1);

    db.delete(parent);

    presult = db.query(new OSQLSynchQuery<Parent>("select * from Parent"));
    cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select * from EmbeddedChild"));

    Assert.assertEquals(presult.size(), 0);
    Assert.assertEquals(cresult.size(), 1);

    db.delete(child);

    presult = db.query(new OSQLSynchQuery<Parent>("select * from Parent"));
    cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select * from EmbeddedChild"));

    Assert.assertEquals(presult.size(), 0);
    Assert.assertEquals(cresult.size(), 0);

    db.close();
  }

  public void testEmbeddedBinary() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.getMetadata().getSchema().reload();

    Account a = new Account(0, "Chris", "Martin");
    a.setThumbnail(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    a = database.save(a);
    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Account aa = (Account) database.load((ORID) a.getRid());
    Assert.assertNotNull(a.getThumbnail());
    Assert.assertNotNull(aa.getThumbnail());
    byte[] b = aa.getThumbnail();
    for (int i = 0; i < 10; ++i)
      Assert.assertEquals(b[i], i);

    // TO REFACTOR OR DELETE SINCE SERIALIZATION AND DESERIALIZATION DON'T APPLY ANYMORE
    // Assert.assertNotNull(aa.getPhoto());
    // b = aa.getPhoto();
    // for (int i = 0; i < 10; ++i)
    // Assert.assertEquals(b[i], i);

    database.close();
  }

  @Test
  public void queryById() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    List<Profile> result1 = database.query(new OSQLSynchQuery<Profile>("select from Profile limit 1"));

    List<Profile> result2 = database.query(new OSQLSynchQuery<Profile>("select from Profile where @rid = ?"), result1.get(0)
        .getId());

    Assert.assertTrue(result2.size() != 0);

    database.close();
  }

}
