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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializer;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.iterator.OObjectIteratorClass;
import com.orientechnologies.orient.object.serialization.OObjectSerializerContext;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;
import com.orientechnologies.orient.test.domain.base.Animal;
import com.orientechnologies.orient.test.domain.base.ComplicatedPerson;
import com.orientechnologies.orient.test.domain.base.JavaCascadeDeleteTestClass;
import com.orientechnologies.orient.test.domain.base.JavaComplexTestClass;
import com.orientechnologies.orient.test.domain.base.JavaSimpleTestClass;
import com.orientechnologies.orient.test.domain.base.Planet;
import com.orientechnologies.orient.test.domain.base.Satellite;
import com.orientechnologies.orient.test.domain.base.SimplePerson;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.Child;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.business.IdentityChild;
import com.orientechnologies.orient.test.domain.customserialization.Sec;
import com.orientechnologies.orient.test.domain.customserialization.SecurityRole;
import com.orientechnologies.orient.test.domain.whiz.Profile;
import javassist.util.proxy.Proxy;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Test(groups = { "record-object", "treeSchemaFull" }, dependsOnGroups = "physicalSchemaFull")
public class ObjectTreeTestSchemaFull extends ObjectDBBaseTest {
  protected long startRecordNumber;
  private long   beginCities;
  protected int  serialized;
  protected int  unserialized;

  public ObjectTreeTestSchemaFull() {
  }

  @Parameters(value = "url")
  public ObjectTreeTestSchemaFull(@Optional String url) {
    super(url, "_objectschema");
  }

  public class CustomClass {
    private String                name;
    private Long                  age;
    private CustomType            custom;
    private List<CustomType>      customTypeList;
    private Set<CustomType>       customTypeSet;
    private Map<Long, CustomType> customTypeMap;

    public CustomClass() {
    }

    public CustomClass(String iName, Long iAge, CustomType iCustom, List<CustomType> iCustomTypeList,
        Set<CustomType> iCustomTypeSet, Map<Long, CustomType> iCustomTypeMap) {
      name = iName;
      age = iAge;
      custom = iCustom;
      customTypeList = iCustomTypeList;
      customTypeSet = iCustomTypeSet;
      customTypeMap = iCustomTypeMap;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Long getAge() {
      return age;
    }

    public void setAge(Long age) {
      this.age = age;
    }

    public CustomType getCustom() {
      return custom;
    }

    public void setCustom(CustomType custom) {
      this.custom = custom;
    }

    public List<CustomType> getCustomTypeList() {
      return customTypeList;
    }

    public void setCustomTypeList(List<CustomType> customTypeList) {
      this.customTypeList = customTypeList;
    }

    public Set<CustomType> getCustomTypeSet() {
      return customTypeSet;
    }

    public void setCustomTypeSet(Set<CustomType> customTypeSet) {
      this.customTypeSet = customTypeSet;
    }

    public Map<Long, CustomType> getCustomTypeMap() {
      return customTypeMap;
    }

    public void setCustomTypeMap(Map<Long, CustomType> customTypeMap) {
      this.customTypeMap = customTypeMap;
    }
  }

  public class CustomType {
    public long value;

    public CustomType() {
    }

    public CustomType(Long iFieldValue) {
      value = iFieldValue;
    }

    public long getValue() {
      return value;
    }

    public void setValue(long value) {
      this.value = value;
    }
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
  }

  @AfterClass
  @Override
  public void afterClass() throws Exception {
    database.close();

    database = createDatabaseInstance(url);
    super.afterClass();
  }

  @BeforeClass
  public void init() {
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");
  }

  @Test
  public void testPool() throws IOException {
    final OObjectDatabaseTx[] dbs = new OObjectDatabaseTx[OObjectDatabasePool.global().getMaxSize()];

    for (int i = 0; i < 10; ++i) {
      for (int db = 0; db < dbs.length; ++db)
        dbs[db] = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      for (int db = 0; db < dbs.length; ++db)
        dbs[db].close();
    }
  }

  @Test
  public void testPersonSaving() {
    final long beginProfiles = database.countClusterElements("Profile");
    beginCities = database.countClusterElements("City");

    Country italy = database.newInstance(Country.class, "Italy");

    Profile garibaldi = database.newInstance(Profile.class, "GGaribaldi", "Giuseppe", "Garibaldi", null);
    garibaldi.setLocation(database.newInstance(Address.class, "Residence", database.newInstance(City.class, italy, "Rome"),
        "Piazza Navona, 1"));

    Profile bonaparte = database.newInstance(Profile.class, "NBonaparte", "Napoleone", "Bonaparte", garibaldi);
    bonaparte.setLocation(database.newInstance(Address.class, "Residence", garibaldi.getLocation().getCity(),
        "Piazza di Spagna, 111"));
    database.save(bonaparte);

    Assert.assertEquals(database.countClusterElements("Profile"), beginProfiles + 2);
  }

  @Test(dependsOnMethods = "testPersonSaving")
  public void testCitySaving() {
    Assert.assertEquals(database.countClusterElements("City"), beginCities + 1);
  }

  @Test(dependsOnMethods = "testCitySaving")
  public void testCityEquality() {
    List<Profile> resultset = database.query(new OSQLSynchQuery<Object>("select from profile where location.city.name = 'Rome'"));
    Assert.assertEquals(resultset.size(), 2);

    Profile p1 = resultset.get(0);
    Profile p2 = resultset.get(1);

    Assert.assertNotSame(p1, p2);
    Assert.assertSame(OObjectEntitySerializer.getDocument((Proxy) p1.getLocation().getCity()),
        OObjectEntitySerializer.getDocument((Proxy) p2.getLocation().getCity()));
  }

  @Test(dependsOnMethods = "testCityEquality")
  public void testSaveCircularLink() {
    Profile winston = database.newInstance(Profile.class, "WChurcill", "Winston", "Churcill", null);
    winston.setLocation(database.newInstance(Address.class, "Residence",
        database.newInstance(City.class, database.newInstance(Country.class, "England"), "London"), "unknown"));

    Profile nicholas = database.newInstance(Profile.class, "NChurcill", "Nicholas ", "Churcill", winston);
    nicholas.setLocation(winston.getLocation());

    nicholas.setInvitedBy(winston);
    winston.setInvitedBy(nicholas);

    database.save(nicholas);
  }

  @Test(dependsOnMethods = "testSaveCircularLink")
  public void testQueryCircular() {
    List<Profile> result = database.query(new OSQLSynchQuery<ODocument>("select * from Profile"));

    Profile parent;
    for (Profile r : result) {

//      System.out.println(r.getNick());

      parent = r.getInvitedBy();

      if (parent != null)
        System.out.println("- parent: " + parent.getName() + " " + parent.getSurname());
    }
  }

  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "testQueryCircular")
  public void testQueryMultiCircular() {
    List<ODocument> result = database.getUnderlying()
        .command(new OSQLSynchQuery<ODocument>("select * from Profile where name = 'Barack' and surname = 'Obama'")).execute();

    Assert.assertEquals(result.size(), 1);

    for (ODocument profile : result) {

//      System.out.println(profile.field("name") + " " + profile.field("surname"));

      final Collection<ODocument> followers = profile.field("followers");

      if (followers != null) {
        for (ODocument follower : followers) {
          Assert.assertTrue(((Collection<ODocument>) follower.field("followings")).contains(profile));

//          System.out.println("- follower: " + follower.field("name") + " " + follower.field("surname") + " (parent: "
//              + follower.field("name") + " " + follower.field("surname") + ")");
        }
      }
    }
  }

  @Test(dependsOnMethods = "testQueryMultiCircular")
  public void simpleEntiyEquals() {
    Set<String> animals = new HashSet<String>();
    animals.add("cat");
    animals.add("dog");
    animals.add("sneake");
    SimplePerson person = new SimplePerson("John", animals);
    SimplePerson proxy = database.save(person);

    Assert.assertEquals(person, proxy);
    Assert.assertEquals(proxy, person);
    database.delete(proxy);
  }

  @Test(dependsOnMethods = "simpleEntiyEquals")
  public void simpleEntiySetEquals() {
    Set<String> animals = new HashSet<String>();
    animals.add("cat");
    animals.add("dog");
    animals.add("sneake");
    SimplePerson person = new SimplePerson("John", animals);
    SimplePerson proxy = database.save(person);

    Assert.assertEquals(person, proxy);
    Assert.assertEquals(proxy, person);
    database.delete(proxy);
  }

  @Test(dependsOnMethods = "simpleEntiySetEquals")
  public void complicatedEntityEquals() {
    Set<Animal> animals = new HashSet<Animal>();
    animals.add(new Animal("cat"));
    animals.add(new Animal("dog"));
    animals.add(new Animal("sneake"));
    ComplicatedPerson person = new ComplicatedPerson("John", animals);
    ComplicatedPerson proxy = database.save(person);

    Assert.assertEquals(person, proxy);
    Assert.assertEquals(proxy, person);
    database.delete(proxy);
  }

  @Test(dependsOnMethods = "complicatedEntityEquals")
  public void complicatedEntitiesSetEquals() {
    Set<Animal> animals = new HashSet<Animal>();
    animals.add(new Animal("cat"));
    animals.add(new Animal("dog"));
    animals.add(new Animal("sneake"));
    ComplicatedPerson person = new ComplicatedPerson("John", animals);
    ComplicatedPerson proxy = database.save(person);

    Assert.assertTrue(proxy.getAnimals().equals(person.getAnimals()));
    Assert.assertTrue(person.getAnimals().equals(proxy.getAnimals()));
    database.delete(proxy);
  }

  @Test(dependsOnMethods = "complicatedEntitiesSetEquals")
  public void simpleProxySelfEquals() {
    Set<String> animals = new HashSet<String>();
    animals.add("cat");
    animals.add("dog");
    animals.add("sneake");
    SimplePerson proxy = database.save(new SimplePerson("John", animals));

    Assert.assertEquals(proxy, proxy);
    database.delete(proxy);
  }

  @Test(dependsOnMethods = "simpleProxySelfEquals")
  public void simpleProxySetsSelfEquals() {
    Set<String> animals = new HashSet<String>();
    animals.add("cat");
    animals.add("dog");
    animals.add("sneake");
    SimplePerson proxy = database.save(new SimplePerson("John", animals));

    // Assert.assertEquals(proxy.getAnimals(), proxy.getAnimals());
    database.delete(proxy);
  }

  @Test(dependsOnMethods = "simpleProxySetsSelfEquals")
  public void complicatedProxySelfEquals() {
    Set<Animal> animals = new HashSet<Animal>();
    animals.add(new Animal("cat"));
    animals.add(new Animal("dog"));
    animals.add(new Animal("sneake"));
    ComplicatedPerson proxy = database.save(new ComplicatedPerson("John", animals));

    Assert.assertEquals(proxy, proxy);
    database.delete(proxy);
  }

  @Test(dependsOnMethods = "complicatedProxySelfEquals")
  public void complicatedProxySetsSelfEquals() {
    Set<Animal> animals = new HashSet<Animal>();
    animals.add(new Animal("cat"));
    animals.add(new Animal("dog"));
    animals.add(new Animal("sneake"));
    ComplicatedPerson proxy = database.save(new ComplicatedPerson("John", animals));

    Assert.assertEquals(proxy.getAnimals(), proxy.getAnimals());
    database.delete(proxy);
  }

  @Test()
  public void testSetEntityDuplication() {
    JavaComplexTestClass test = database.newInstance(JavaComplexTestClass.class);
    for (int i = 0; i < 100; i++) {
      IdentityChild child = database.newInstance(IdentityChild.class);
      child.setName(String.valueOf(i));
      test.getDuplicationTestSet().add(child);
    }
    Assert.assertNotNull(test.getDuplicationTestSet());
    Assert.assertEquals(test.getDuplicationTestSet().size(), 1);
    database.save(test);
    // Assert.assertEquals(test.getSet().size(), 100);
    ORID rid = database.getIdentity(test);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    test = database.load(rid);
    Assert.assertNotNull(test.getDuplicationTestSet());
    Assert.assertEquals(test.getDuplicationTestSet().size(), 1);
    for (int i = 0; i < 100; i++) {
      IdentityChild child = new IdentityChild();
      child.setName(String.valueOf(i));
      test.getDuplicationTestSet().add(child);
    }
    Assert.assertEquals(test.getDuplicationTestSet().size(), 1);
    database.save(test);
    rid = database.getIdentity(test);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    test = database.load(rid);
    Assert.assertNotNull(test.getDuplicationTestSet());
    Assert.assertEquals(test.getDuplicationTestSet().size(), 1);
    List<IdentityChild> childs = database.query(new OSQLSynchQuery<IdentityChild>("select from IdentityChild"));
    Assert.assertEquals(childs.size(), 1);
    database.delete(test);
  }

  @Test()
  public void testSetFieldSize() {
    JavaComplexTestClass test = database.newInstance(JavaComplexTestClass.class);
    for (int i = 0; i < 100; i++) {
      Child child = database.newInstance(Child.class);
      child.setName(String.valueOf(i));
      test.getSet().add(child);
    }
    Assert.assertNotNull(test.getSet());
    Assert.assertEquals(test.getSet().size(), 100);
    database.save(test);
    // Assert.assertEquals(test.getSet().size(), 100);
    ORID rid = database.getIdentity(test);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    test = database.load(rid);
    Assert.assertNotNull(test.getSet());
    Iterator<Child> it = test.getSet().iterator();
    while (it.hasNext()) {
      Child child = it.next();
      Assert.assertNotNull(child.getName());
      Assert.assertTrue(Integer.valueOf(child.getName()) < 100);
      Assert.assertTrue(Integer.valueOf(child.getName()) >= 0);
    }
    Assert.assertEquals(test.getSet().size(), 100);
    database.delete(test);
  }

  @Test(dependsOnMethods = "testQueryMultiCircular")
  public void testCollectionsRemove() {
    JavaComplexTestClass a = database.newInstance(JavaComplexTestClass.class);

    // LIST TEST
    Child first = database.newInstance(Child.class);
    first.setName("1");
    Child second = database.newInstance(Child.class);
    second.setName("2");
    Child third = database.newInstance(Child.class);
    third.setName("3");
    Child fourth = database.newInstance(Child.class);
    fourth.setName("4");
    Child fifth = database.newInstance(Child.class);
    fifth.setName("5");

    a.getList().add(first);
    a.getList().add(second);
    a.getList().add(third);
    a.getList().add(fourth);
    a.getList().add(fifth);

    a.getSet().add(first);
    a.getSet().add(second);
    a.getSet().add(third);
    a.getSet().add(fourth);
    a.getSet().add(fifth);

    a.getList().remove(third);
    a.getSet().remove(fourth);

    Assert.assertEquals(a.getList().size(), 4);
    Assert.assertEquals(a.getSet().size(), 4);
    ODocument doc = database.getRecordByUserObject(a, false);
    Assert.assertEquals(((Collection<?>) doc.field("list")).size(), 4);
    Assert.assertEquals(((Collection<?>) doc.field("set")).size(), 4);

    a = database.save(a);
    ORID rid = database.getIdentity(a);

    Assert.assertEquals(a.getList().size(), 4);
    Assert.assertEquals(a.getSet().size(), 4);
    doc = database.getRecordByUserObject(a, false);
    Assert.assertEquals(((Collection<?>) doc.field("list")).size(), 4);
    Assert.assertEquals(((Collection<?>) doc.field("set")).size(), 4);

    database.close();

    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    JavaComplexTestClass loadedObj = database.load(rid);

    Assert.assertEquals(loadedObj.getList().size(), 4);
    Assert.assertEquals(loadedObj.getSet().size(), 4);
    doc = database.getRecordByUserObject(loadedObj, false);
    Assert.assertEquals(((Collection<?>) doc.field("list")).size(), 4);
    Assert.assertEquals(((Collection<?>) doc.field("set")).size(), 4);

    database.delete(rid);

  }

  @Test(dependsOnMethods = "testCollectionsRemove")
  public void testCascadeDeleteSimpleObject() {
    JavaCascadeDeleteTestClass test = database.newInstance(JavaCascadeDeleteTestClass.class);
    JavaSimpleTestClass simple = database.newInstance(JavaSimpleTestClass.class);
    simple.setText("asdasd");
    test.setSimpleClass(simple);
    database.save(test);
    ORID testRid = database.getRecordByUserObject(test, false).getIdentity();
    ORID simpleRid = database.getRecordByUserObject(simple, false).getIdentity();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.delete(testRid);
    simple = database.load(simpleRid);
    Assert.assertNull(simple);

    // TEST SET NULL
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    test = database.newInstance(JavaCascadeDeleteTestClass.class);
    simple = database.newInstance(JavaSimpleTestClass.class);
    simple.setText("asdasd");
    test.setSimpleClass(simple);
    database.save(test);
    testRid = database.getRecordByUserObject(test, false).getIdentity();
    simpleRid = database.getRecordByUserObject(simple, false).getIdentity();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    test.setSimpleClass(null);
    database.save(test);
    simple = database.load(simpleRid);
    Assert.assertNull(simple);
    database.delete(test);

    // TEST CHANGE NEW RECORD
    test = database.newInstance(JavaCascadeDeleteTestClass.class);
    simple = database.newInstance(JavaSimpleTestClass.class);
    simple.setText("asdasd");
    test.setSimpleClass(simple);
    database.save(test);
    testRid = database.getRecordByUserObject(test, false).getIdentity();
    simpleRid = database.getRecordByUserObject(simple, false).getIdentity();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    simple = database.newInstance(JavaSimpleTestClass.class);
    database.save(simple);
    test.setSimpleClass(simple);
    database.save(test);
    simple = database.load(simpleRid);
    Assert.assertNull(simple);
    database.delete(test);
  }

  @Test(dependsOnMethods = "testCascadeDeleteSimpleObject")
  public void testCascadeDeleteCollections() {
    JavaCascadeDeleteTestClass test = database.newInstance(JavaCascadeDeleteTestClass.class);
    Child listChild1 = database.newInstance(Child.class);
    listChild1.setName("list1");
    test.getList().add(listChild1);
    Child listChild2 = database.newInstance(Child.class);
    listChild2.setName("list2");
    test.getList().add(listChild2);
    Child listChild3 = database.newInstance(Child.class);
    listChild3.setName("list3");
    test.getList().add(listChild3);

    Child setChild1 = database.newInstance(Child.class);
    setChild1.setName("set1");
    test.getSet().add(setChild1);
    Child setChild2 = database.newInstance(Child.class);
    setChild2.setName("set2");
    test.getSet().add(setChild2);
    Child setChild3 = database.newInstance(Child.class);
    setChild3.setName("set3");
    test.getSet().add(setChild3);

    database.save(test);
    ORID testRid = database.getRecordByUserObject(test, false).getIdentity();
    ORID list1Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
    ORID list2Rid = database.getRecordByUserObject(listChild2, false).getIdentity();
    ORID list3Rid = database.getRecordByUserObject(listChild3, false).getIdentity();
    ORID set1Rid = database.getRecordByUserObject(setChild1, false).getIdentity();
    ORID set2Rid = database.getRecordByUserObject(setChild2, false).getIdentity();
    ORID set3Rid = database.getRecordByUserObject(setChild3, false).getIdentity();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.delete(testRid);
    listChild1 = database.load(list1Rid);
    listChild2 = database.load(list2Rid);
    listChild3 = database.load(list3Rid);
    setChild1 = database.load(set1Rid);
    setChild2 = database.load(set2Rid);
    setChild3 = database.load(set3Rid);
    Assert.assertNull(listChild1);
    Assert.assertNull(listChild2);
    Assert.assertNull(listChild3);
    Assert.assertNull(setChild1);
    Assert.assertNull(setChild2);
    Assert.assertNull(setChild3);

    // LIST UPDATE TEST
    test = database.newInstance(JavaCascadeDeleteTestClass.class);
    listChild1 = database.newInstance(Child.class);
    listChild1.setName("list1");
    test.getList().add(listChild1);
    listChild2 = database.newInstance(Child.class);
    listChild2.setName("list2");
    test.getList().add(listChild2);
    listChild3 = database.newInstance(Child.class);
    listChild3.setName("list3");
    test.getList().add(listChild3);
    Child listChild4 = database.newInstance(Child.class);
    listChild4.setName("list4");
    test.getList().add(listChild4);

    setChild1 = database.newInstance(Child.class);
    setChild1.setName("set1");
    test.getSet().add(setChild1);
    setChild2 = database.newInstance(Child.class);
    setChild2.setName("set2");
    test.getSet().add(setChild2);
    setChild3 = database.newInstance(Child.class);
    setChild3.setName("set3");
    test.getSet().add(setChild3);
    Child setChild4 = database.newInstance(Child.class);
    setChild4.setName("set4");
    test.getSet().add(setChild4);

    database.save(test);
    testRid = database.getRecordByUserObject(test, false).getIdentity();
    list1Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
    list2Rid = database.getRecordByUserObject(listChild2, false).getIdentity();
    list3Rid = database.getRecordByUserObject(listChild3, false).getIdentity();
    ORID list4Rid = database.getRecordByUserObject(listChild4, false).getIdentity();
    set1Rid = database.getRecordByUserObject(setChild1, false).getIdentity();
    set2Rid = database.getRecordByUserObject(setChild2, false).getIdentity();
    set3Rid = database.getRecordByUserObject(setChild3, false).getIdentity();
    ORID set4Rid = database.getRecordByUserObject(setChild4, false).getIdentity();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    test = database.load(testRid);
    test.getList().remove(listChild4);
    test.getList().remove(0);
    test.getList().remove(listChild3);
    test.getList().add(listChild4);
    Iterator<Child> it = test.getList().iterator();
    it.next();
    it.remove();
    test.getSet().remove(setChild1);
    test.getSet().remove(setChild4);
    Assert.assertFalse(test.getSet().contains(setChild1));
    Assert.assertFalse(test.getSet().contains(setChild4));
    it = test.getSet().iterator();
    it.next();
    it.remove();
    Assert.assertTrue((!test.getSet().contains(setChild2) || !test.getSet().contains(setChild3)));
    test.getSet().add(setChild4);
    database.save(test);
    test = database.load(testRid);
    Assert.assertTrue(!test.getList().contains(listChild3));
    listChild1 = database.load(list1Rid);
    listChild2 = database.load(list2Rid);
    listChild3 = database.load(list3Rid);
    listChild4 = database.load(list4Rid);
    setChild1 = database.load(set1Rid);
    setChild2 = database.load(set2Rid);
    setChild3 = database.load(set3Rid);
    setChild4 = database.load(set4Rid);
    Assert.assertNull(listChild1);
    Assert.assertNull(listChild2);
    Assert.assertNull(listChild3);
    Assert.assertNotNull(listChild4);
    Assert.assertNull(setChild1);
    Assert.assertTrue((setChild3 != null && setChild2 == null) || (setChild3 == null && setChild2 != null));
    Assert.assertNotNull(setChild4);
    database.delete(test);
  }

  @SuppressWarnings("unused")
  @Test(dependsOnMethods = "testCascadeDeleteCollections")
  public void testDeleteRecordOutsideCollection() {
    JavaCascadeDeleteTestClass test = database.newInstance(JavaCascadeDeleteTestClass.class);
    Child listChild1 = database.newInstance(Child.class);
    listChild1.setName("list1");
    test.getList().add(listChild1);
    Child listChild2 = database.newInstance(Child.class);
    listChild2.setName("list2");
    test.getList().add(listChild2);
    Child listChild3 = database.newInstance(Child.class);
    listChild3.setName("list3");
    test.getList().add(listChild3);

    Child setChild1 = database.newInstance(Child.class);
    setChild1.setName("set1");
    test.getSet().add(setChild1);
    Child setChild2 = database.newInstance(Child.class);
    setChild2.setName("set2");
    test.getSet().add(setChild2);
    Child setChild3 = database.newInstance(Child.class);
    setChild3.setName("set3");
    test.getSet().add(setChild3);

    database.save(test);
    ORID testRid = database.getRecordByUserObject(test, false).getIdentity();
    ORID list1Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
    ORID set2Rid = database.getRecordByUserObject(setChild2, false).getIdentity();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    database.delete(list1Rid);
    database.delete(set2Rid);
    test = database.load(testRid);
    try {
      for (int i = 0; i < test.getList().size(); i++) {
        if (i == 0) {
          Assert.assertNull(test.getList().get(i));
        } else {
          Assert.assertNotNull(test.getList().get(i));
        }
      }
      for (Child c : test.getSet()) {
      }
    } catch (NullPointerException npe) {
      Assert.fail("NullPointer on list retrieving that shouldn't happen");
    }

    database.delete(test);
  }

  @Test(dependsOnMethods = "testCascadeDeleteCollections")
  public void testCascadeDeleteMap() {
    JavaCascadeDeleteTestClass test = database.newInstance(JavaCascadeDeleteTestClass.class);
    Child mapChild1 = database.newInstance(Child.class);
    mapChild1.setName("map1");
    test.getChildren().put("1", mapChild1);
    Child mapChild2 = database.newInstance(Child.class);
    mapChild2.setName("map2");
    test.getChildren().put("2", mapChild2);
    Child mapChild3 = database.newInstance(Child.class);
    mapChild3.setName("map3");
    test.getChildren().put("3", mapChild3);

    database.save(test);
    ORID testRid = database.getRecordByUserObject(test, false).getIdentity();
    ORID map1Rid = database.getRecordByUserObject(mapChild1, false).getIdentity();
    ORID map2Rid = database.getRecordByUserObject(mapChild2, false).getIdentity();
    ORID map3Rid = database.getRecordByUserObject(mapChild3, false).getIdentity();

    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");


    database.delete(testRid);
    mapChild1 = database.load(map1Rid);
    mapChild2 = database.load(map2Rid);
    mapChild3 = database.load(map3Rid);
    Assert.assertNull(mapChild1);
    Assert.assertNull(mapChild2);
    Assert.assertNull(mapChild3);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");


    // MAP UPDATE TEST
    test = database.newInstance(JavaCascadeDeleteTestClass.class);
    mapChild1 = database.newInstance(Child.class);
    mapChild1.setName("map1");
    test.getChildren().put("1", mapChild1);
    mapChild2 = database.newInstance(Child.class);
    mapChild2.setName("map2");
    test.getChildren().put("2", mapChild2);
    mapChild3 = database.newInstance(Child.class);
    mapChild3.setName("map3");
    test.getChildren().put("3", mapChild3);
    Child mapChild4 = database.newInstance(Child.class);
    mapChild4.setName("map4");
    test.getChildren().put("4", mapChild4);
    Child mapChild5 = database.newInstance(Child.class);
    mapChild5.setName("map5");
    test.getChildren().put("5", mapChild5);

    database.save(test);
    testRid = database.getIdentity(test);
    map1Rid = database.getRecordByUserObject(mapChild1, false).getIdentity();
    map2Rid = database.getRecordByUserObject(mapChild2, false).getIdentity();
    map3Rid = database.getRecordByUserObject(mapChild3, false).getIdentity();
    ORID map4Rid = database.getRecordByUserObject(mapChild4, false).getIdentity();
    ORID map5Rid = database.getRecordByUserObject(mapChild5, false).getIdentity();
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    test = database.load(testRid);
    Assert.assertNotNull(test.getChildren().get("1"));
    Assert.assertNotNull(test.getChildren().get("2"));
    Assert.assertNotNull(test.getChildren().get("3"));
    Assert.assertNotNull(test.getChildren().get("4"));
    Assert.assertNotNull(test.getChildren().get("5"));
    test.getChildren().remove("5");
    test.getChildren().put("1", mapChild1);
    test.getChildren().put("2", mapChild1);
    test.getChildren().put("3", null);
    test.getChildren().remove("4");
    test.getChildren().put("3", mapChild5);
    database.save(test);
    mapChild1 = database.load(map1Rid);
    mapChild2 = database.load(map2Rid);
    mapChild3 = database.load(map3Rid);
    mapChild4 = database.load(map4Rid);
    mapChild5 = database.load(map5Rid);
    Assert.assertNotNull(mapChild1);
    Assert.assertNull(mapChild2);
    Assert.assertNull(mapChild3);
    Assert.assertNull(mapChild4);
    Assert.assertNotNull(mapChild5);
    database.delete(test);
  }

  @Test(dependsOnMethods = "testPool")
  public void testCustomTypes() {
    OObjectSerializerContext serializerContext = new OObjectSerializerContext();
    serializerContext.bind(new OObjectSerializer<CustomType, Long>() {

      @Override
      public Long serializeFieldValue(Class<?> itype, CustomType iFieldValue) {
        serialized++;
        return iFieldValue.value;
      }

      @Override
      public CustomType unserializeFieldValue(Class<?> itype, Long iFieldValue) {
        unserialized++;
        return new CustomType(iFieldValue);
      }

    }, database);
    OObjectSerializerHelper.bindSerializerContext(null, serializerContext);
    database.getEntityManager().registerEntityClass(CustomClass.class);

    if (!database.getMetadata().getSchema().existsClass("CustomClass"))
      database.getMetadata().getSchema().createClass("CustomClass");

    List<CustomType> customTypesList = new ArrayList<CustomType>();
    customTypesList.add(new CustomType(102L));

    Set<CustomType> customTypeSet = new HashSet<CustomType>();
    customTypeSet.add(new CustomType(103L));

    Map<Long, CustomType> customTypeMap = new HashMap<Long, CustomType>();
    customTypeMap.put(1L, new CustomType(104L));

    CustomClass pojo = new CustomClass("test", 33L, new CustomType(101L), customTypesList, customTypeSet, customTypeMap);
    // init counters
    serialized = 0;
    unserialized = 0;
    pojo = database.save(pojo);
    Assert.assertEquals(serialized, 4);
    Assert.assertEquals(unserialized, 0);

    pojo = database.reload(pojo);
    Assert.assertEquals(unserialized, 0);

    pojo.getCustom();
    Assert.assertEquals(unserialized, 1);
    Assert.assertTrue(pojo.getCustom() instanceof CustomType);

    pojo.getCustomTypeList().iterator().next();
    Assert.assertEquals(unserialized, 2);
    Assert.assertTrue(pojo.getCustomTypeList().iterator().next() instanceof CustomType);
    unserialized--;

    pojo.getCustomTypeSet().iterator().next();
    Assert.assertEquals(unserialized, 3);
    Assert.assertTrue(pojo.getCustomTypeSet().iterator().next() instanceof CustomType);
    unserialized--;

    pojo.getCustomTypeMap().get(1L);
    Assert.assertEquals(serialized, 4);
    Assert.assertEquals(unserialized, 4);
    Assert.assertTrue(pojo.getCustomTypeMap().get(1L) instanceof CustomType);
  }

  @Test(dependsOnMethods = "testCustomTypes")
  public void testCustomTypesDatabaseNewInstance() {
    OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    ORID rid = null;
    try {
      // init counters
      serialized = 0;
      unserialized = 0;

      List<CustomType> customTypesList = new ArrayList<CustomType>();
      customTypesList.add(new CustomType(102L));

      Set<CustomType> customTypeSet = new HashSet<CustomType>();
      customTypeSet.add(new CustomType(103L));

      Map<Long, CustomType> customTypeMap = new HashMap<Long, CustomType>();
      customTypeMap.put(1L, new CustomType(104L));

      CustomClass pojo = database.newInstance(CustomClass.class, "test", 33L, new CustomType(101L), customTypesList, customTypeSet,
          customTypeMap);
      Assert.assertEquals(serialized, 4);
      Assert.assertEquals(unserialized, 0);

      pojo = database.save(pojo);

      rid = database.getIdentity(pojo);

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

      pojo = database.load(rid);
      Assert.assertEquals(unserialized, 0);

      pojo.getCustom();
      Assert.assertEquals(unserialized, 1);
      Assert.assertTrue(pojo.getCustom() instanceof CustomType);

      pojo.getCustomTypeList().iterator().next();
      Assert.assertEquals(unserialized, 2);
      Assert.assertTrue(pojo.getCustomTypeList().iterator().next() instanceof CustomType);
      unserialized--;

      pojo.getCustomTypeSet().iterator().next();
      Assert.assertEquals(unserialized, 3);
      Assert.assertTrue(pojo.getCustomTypeSet().iterator().next() instanceof CustomType);
      unserialized--;

      pojo.getCustomTypeMap().get(1L);
      Assert.assertEquals(serialized, 4);
      Assert.assertEquals(unserialized, 4);
      Assert.assertTrue(pojo.getCustomTypeMap().get(1L) instanceof CustomType);
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testCustomTypesDatabaseNewInstance")
  public void testEnumListWithCustomTypes() {
    OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    ORID rid = null;
    try {
      OObjectSerializerContext serializerContext = new OObjectSerializerContext();
      serializerContext.bind(new OObjectSerializer<SecurityRole, String>() {

        @Override
        public Object serializeFieldValue(Class<?> type, SecurityRole role) {
          return role.name();
        }

        @Override
        public Object unserializeFieldValue(Class<?> type, String str) {
          return SecurityRole.getByName(str);
        }
      }, database);

      OObjectSerializerHelper.bindSerializerContext(null, serializerContext);

      database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.customserialization");

      Sec s = new Sec();
      s.getSecurityRoleList().add(SecurityRole.LOGIN);

      Assert.assertTrue(s.getSecurityRoleList().contains(SecurityRole.LOGIN));

      s = database.save(s);
      rid = database.getRecordByUserObject(s, false).getIdentity();

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

      s = database.load(rid);

      Assert.assertTrue(s.getSecurityRoleList().contains(SecurityRole.LOGIN));
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testEnumListWithCustomTypes")
  public void childUpdateTest() {
    OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Planet p = database.newInstance(Planet.class);
    Satellite sat = database.newInstance(Satellite.class);
    p.setName("Earth");
    p.setDistanceSun(1000);
    sat.setDiameter(50);
    p.addSatellite(sat);
    database.save(p);
    ORID rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    p = database.load(rid);
    sat = p.getSatellites().get(0);
    Assert.assertEquals(sat.getDiameter(), 50);
    Assert.assertEquals(p.getDistanceSun(), 1000);
    Assert.assertEquals(p.getName(), "Earth");
    sat.setDiameter(500);
    // p.addSatellite(new Satellite("Moon", 70));
    // db.save(sat);
    database.save(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    p = database.load(rid);
    sat = p.getSatellites().get(0);
    Assert.assertEquals(sat.getDiameter(), 500);
    Assert.assertEquals(p.getDistanceSun(), 1000);
    Assert.assertEquals(p.getName(), "Earth");
    database.close();
  }

  @Test(dependsOnMethods = "childUpdateTest")
  public void childNLevelUpdateTest() {
    OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Planet p = database.newInstance(Planet.class);
    Planet near = database.newInstance(Planet.class);
    Satellite sat = database.newInstance(Satellite.class);
    Satellite satNear = database.newInstance(Satellite.class);
    sat.setDiameter(50);
    sat.setNear(near);
    satNear.setDiameter(10);
    near.addSatellite(satNear);
    p.addSatellite(sat);
    database.save(p);
    ORID rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    p = database.load(rid);
    sat = p.getSatellites().get(0);
    near = sat.getNear();
    satNear = near.getSatellites().get(0);
    Assert.assertEquals(satNear.getDiameter(), 10);
    satNear.setDiameter(100);
    // p.addSatellite(new Satellite("Moon", 70));
    // db.save(sat);
    database.save(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    p = database.load(rid);
    sat = p.getSatellites().get(0);
    near = sat.getNear();
    satNear = near.getSatellites().get(0);
    Assert.assertEquals(satNear.getDiameter(), 100);
    database.close();
  }

  @Test(dependsOnMethods = "childNLevelUpdateTest")
  public void childMapUpdateTest() {
    OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Planet p = database.newInstance(Planet.class);
    p.setName("Earth");
    p.setDistanceSun(1000);
    Satellite sat = database.newInstance(Satellite.class);
    sat.setDiameter(50);
    sat.setName("Moon");
    p.addSatelliteMap(sat);
    database.save(p);
    Assert.assertEquals(p.getDistanceSun(), 1000);
    Assert.assertEquals(p.getName(), "Earth");
    ORID rid = database.getIdentity(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    p = database.load(rid);
    sat = p.getSatellitesMap().get("Moon");
    Assert.assertEquals(p.getDistanceSun(), 1000);
    Assert.assertEquals(p.getName(), "Earth");
    Assert.assertEquals(sat.getDiameter(), 50);
    sat.setDiameter(500);
    database.save(p);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    p = database.load(rid);
    sat = p.getSatellitesMap().get("Moon");
    Assert.assertEquals(sat.getDiameter(), 500);
    Assert.assertEquals(p.getDistanceSun(), 1000);
    Assert.assertEquals(p.getName(), "Earth");
    database.close();
  }

  @Test(dependsOnMethods = "childMapUpdateTest")
  public void childMapNLevelUpdateTest() {
    OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    Planet jupiter = database.newInstance(Planet.class);
    jupiter.setName("Jupiter");
    jupiter.setDistanceSun(3000);
    Planet mercury = database.newInstance(Planet.class);
    mercury.setName("Mercury");
    mercury.setDistanceSun(5000);
    Satellite jupiterMoon = database.newInstance(Satellite.class);
    Satellite mercuryMoon = database.newInstance(Satellite.class);
    jupiterMoon.setDiameter(50);
    jupiterMoon.setNear(mercury);
    jupiterMoon.setName("JupiterMoon");
    mercuryMoon.setDiameter(10);
    mercuryMoon.setName("MercuryMoon");
    mercury.addSatelliteMap(mercuryMoon);
    jupiter.addSatelliteMap(jupiterMoon);
    database.save(jupiter);
    ORID rid = database.getIdentity(jupiter);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    jupiter = database.load(rid);
    jupiterMoon = jupiter.getSatellitesMap().get("JupiterMoon");
    mercury = jupiterMoon.getNear();
    mercuryMoon = mercury.getSatellitesMap().get("MercuryMoon");
    Assert.assertEquals(mercuryMoon.getDiameter(), 10);
    Assert.assertEquals(mercuryMoon.getName(), "MercuryMoon");
    Assert.assertEquals(jupiterMoon.getDiameter(), 50);
    Assert.assertEquals(jupiterMoon.getName(), "JupiterMoon");
    Assert.assertEquals(jupiter.getName(), "Jupiter");
    Assert.assertEquals(jupiter.getDistanceSun(), 3000);
    Assert.assertEquals(mercury.getName(), "Mercury");
    Assert.assertEquals(mercury.getDistanceSun(), 5000);
    mercuryMoon.setDiameter(100);
    // p.addSatellite(new Satellite("Moon", 70));
    // db.save(sat);
    database.save(jupiter);
    database.close();
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    jupiter = database.load(rid);
    jupiterMoon = jupiter.getSatellitesMap().get("JupiterMoon");
    mercury = jupiterMoon.getNear();
    mercuryMoon = mercury.getSatellitesMap().get("MercuryMoon");
    Assert.assertEquals(mercuryMoon.getDiameter(), 100);
    Assert.assertEquals(mercuryMoon.getName(), "MercuryMoon");
    Assert.assertEquals(jupiterMoon.getDiameter(), 50);
    Assert.assertEquals(jupiterMoon.getName(), "JupiterMoon");
    Assert.assertEquals(jupiter.getName(), "Jupiter");
    Assert.assertEquals(jupiter.getDistanceSun(), 3000);
    Assert.assertEquals(mercury.getName(), "Mercury");
    Assert.assertEquals(mercury.getDistanceSun(), 5000);
    database.close();
  }

  @Test
  public void iteratorShouldTerminate() {
    OObjectDatabaseTx db = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      db.getEntityManager().registerEntityClass(Profile.class);

      db.begin();
      Profile person = new Profile();
      person.setNick("Guy1");
      person.setName("Guy");
      person.setSurname("Ritchie");
      person = db.save(person);
      db.commit();

      db.begin();
      db.delete(person);
      db.commit();

      db.begin();
      Profile person2 = new Profile();
      person2.setNick("Guy2");
      person2.setName("Guy");
      person2.setSurname("Brush");
      person2 = db.save(person2);
      OObjectIteratorClass<Profile> it = db.browseClass(Profile.class);
      while (it.hasNext()) {
        System.out.println(it.next());
      }

      db.commit();
    } finally {
      db.close();
    }
  }
}
