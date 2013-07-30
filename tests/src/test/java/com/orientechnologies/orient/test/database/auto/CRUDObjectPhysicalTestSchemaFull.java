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
import java.util.Calendar;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import javassist.util.proxy.Proxy;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.object.OLazyObjectSetInterface;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.object.db.ODatabaseObjectTx;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.iterator.OObjectIteratorClass;
import com.orientechnologies.orient.object.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.test.domain.base.Agenda;
import com.orientechnologies.orient.test.domain.base.EmbeddedChild;
import com.orientechnologies.orient.test.domain.base.EnumTest;
import com.orientechnologies.orient.test.domain.base.Event;
import com.orientechnologies.orient.test.domain.base.IdObject;
import com.orientechnologies.orient.test.domain.base.Instrument;
import com.orientechnologies.orient.test.domain.base.JavaComplexTestClass;
import com.orientechnologies.orient.test.domain.base.JavaNoGenericCollectionsTestClass;
import com.orientechnologies.orient.test.domain.base.JavaSimpleArraysTestClass;
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

@Test(groups = { "crud", "object", "schemafull", "physicalSchemaFull" }, dependsOnGroups = "inheritanceSchemaFull")
public class CRUDObjectPhysicalTestSchemaFull {
  protected static final int TOT_RECORDS = 100;
  protected long             startRecordNumber;
  private OObjectDatabaseTx  database;
  private City               rome        = new City(new Country("Italy"), "Rome");
  private String             url;

  @Parameters(value = "url")
  public CRUDObjectPhysicalTestSchemaFull(String iURL) {
    url = iURL + "_objectschema";
  }

  @Test
  public void create() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    database.command(new OCommandSQL("delete from Company")).execute();
    database.setAutomaticSchemaGeneration(true);
    try {
      database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
      database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");
      database.setAutomaticSchemaGeneration(false);
      database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");

      startRecordNumber = database.countClusterElements("Account");

      Account account;

      for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
        account = new Account((int) i, "Bill", "Gates");
        account.setBirthDate(new Date());
        account.setSalary(i + 300.10f);
        account.getAddresses().add(new Address("Residence", rome, "Piazza Navona, 1"));
        database.save(account);
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "create", expectedExceptions = UnsupportedOperationException.class)
  public void testReleasedPoolDatabase() {
    database.open("admin", "admin");
  }

  @Test(dependsOnMethods = "testReleasedPoolDatabase")
  public void testCreate() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      Assert.assertEquals(database.countClusterElements("Account") - startRecordNumber, TOT_RECORDS);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void testSimpleTypes() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testSimpleArrayTypes() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      JavaSimpleArraysTestClass javaObj = database.newInstance(JavaSimpleArraysTestClass.class);
      Assert.assertEquals(javaObj.getText()[0], "initTest");
      String[] textArray = new String[10];
      EnumTest[] enumerationArray = new EnumTest[10];
      int[] intArray = new int[10];
      long[] longArray = new long[10];
      double[] doubleArray = new double[10];
      float[] floatArray = new float[10];
      byte[] byteArray = new byte[10];
      boolean[] booleanArray = new boolean[10];
      Date[] dateArray = new Date[10];
      Calendar cal = Calendar.getInstance();
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.MILLISECOND, 0);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.YEAR, 1900);
      cal.set(Calendar.MONTH, Calendar.JANUARY);
      for (int i = 0; i < 10; i++) {
        textArray[i] = i + "";
        intArray[i] = i;
        longArray[i] = (long) i;
        doubleArray[i] = (double) i;
        floatArray[i] = (float) i;
        byteArray[i] = (byte) i;
        booleanArray[i] = (i % 2 == 0);
        enumerationArray[i] = (i % 2 == 0) ? EnumTest.ENUM2 : ((i % 3 == 0) ? EnumTest.ENUM3 : EnumTest.ENUM1);
        cal.set(Calendar.DAY_OF_MONTH, (i + 1));
        dateArray[i] = cal.getTime();
      }
      javaObj.setText(textArray);
      javaObj.setByteSimple(byteArray);
      javaObj.setDateField(dateArray);
      javaObj.setDoubleSimple(doubleArray);
      javaObj.setEnumeration(enumerationArray);
      javaObj.setFlagSimple(booleanArray);
      javaObj.setFloatSimple(floatArray);
      javaObj.setLongSimple(longArray);
      javaObj.setNumberSimple(intArray);

      ODocument doc = database.getRecordByUserObject(javaObj, false);
      Assert.assertNotNull(doc.field("text"));
      Assert.assertNotNull(doc.field("enumeration"));
      Assert.assertNotNull(doc.field("numberSimple"));
      Assert.assertNotNull(doc.field("longSimple"));
      Assert.assertNotNull(doc.field("doubleSimple"));
      Assert.assertNotNull(doc.field("floatSimple"));
      Assert.assertNotNull(doc.field("byteSimple"));
      Assert.assertNotNull(doc.field("flagSimple"));
      Assert.assertNotNull(doc.field("dateField"));

      JavaSimpleArraysTestClass savedJavaObj = database.save(javaObj);
      ORID id = database.getIdentity(savedJavaObj);
      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      JavaSimpleArraysTestClass loadedJavaObj = database.load(id);
      doc = database.getRecordByUserObject(loadedJavaObj, false);
      Assert.assertNotNull(doc.field("text"));
      Assert.assertNotNull(doc.field("enumeration"));
      Assert.assertNotNull(doc.field("numberSimple"));
      Assert.assertNotNull(doc.field("longSimple"));
      Assert.assertNotNull(doc.field("doubleSimple"));
      Assert.assertNotNull(doc.field("floatSimple"));
      Assert.assertNotNull(doc.field("byteSimple"));
      Assert.assertNotNull(doc.field("flagSimple"));
      Assert.assertNotNull(doc.field("dateField"));

      Assert.assertEquals(loadedJavaObj.getText().length, 10);
      Assert.assertEquals(loadedJavaObj.getNumberSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getLongSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getDoubleSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getFloatSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getByteSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getFlagSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getEnumeration().length, 10);
      Assert.assertEquals(loadedJavaObj.getDateField().length, 10);

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals(loadedJavaObj.getText()[i], i + "");
        Assert.assertEquals(loadedJavaObj.getNumberSimple()[i], i);
        Assert.assertEquals(loadedJavaObj.getLongSimple()[i], (long) i);
        Assert.assertEquals(loadedJavaObj.getDoubleSimple()[i], (double) i);
        Assert.assertEquals(loadedJavaObj.getFloatSimple()[i], (float) i);
        Assert.assertEquals(loadedJavaObj.getByteSimple()[i], (byte) i);
        Assert.assertEquals(loadedJavaObj.getFlagSimple()[i], (i % 2 == 0));
        EnumTest enumCheck = (i % 2 == 0) ? EnumTest.ENUM2 : ((i % 3 == 0) ? EnumTest.ENUM3 : EnumTest.ENUM1);
        Assert.assertEquals(loadedJavaObj.getEnumeration()[i], enumCheck);
        cal.set(Calendar.DAY_OF_MONTH, (i + 1));
        Assert.assertEquals(loadedJavaObj.getDateField()[i], cal.getTime());
      }

      for (int i = 0; i < 10; i++) {
        int j = i + 10;
        textArray[i] = j + "";
        intArray[i] = j;
        longArray[i] = (long) j;
        doubleArray[i] = (double) j;
        floatArray[i] = (float) j;
        byteArray[i] = (byte) j;
        booleanArray[i] = (j % 2 == 0);
        enumerationArray[i] = (j % 2 == 0) ? EnumTest.ENUM2 : ((j % 3 == 0) ? EnumTest.ENUM3 : EnumTest.ENUM1);
        cal.set(Calendar.DAY_OF_MONTH, (j + 1));
        dateArray[i] = cal.getTime();
      }
      loadedJavaObj.setText(textArray);
      loadedJavaObj.setByteSimple(byteArray);
      loadedJavaObj.setDateField(dateArray);
      loadedJavaObj.setDoubleSimple(doubleArray);
      loadedJavaObj.setEnumeration(enumerationArray);
      loadedJavaObj.setFlagSimple(booleanArray);
      loadedJavaObj.setFloatSimple(floatArray);
      loadedJavaObj.setLongSimple(longArray);
      loadedJavaObj.setNumberSimple(intArray);

      doc = database.getRecordByUserObject(javaObj, false);
      Assert.assertNotNull(doc.field("text"));
      Assert.assertNotNull(doc.field("enumeration"));
      Assert.assertNotNull(doc.field("numberSimple"));
      Assert.assertNotNull(doc.field("longSimple"));
      Assert.assertNotNull(doc.field("doubleSimple"));
      Assert.assertNotNull(doc.field("floatSimple"));
      Assert.assertNotNull(doc.field("byteSimple"));
      Assert.assertNotNull(doc.field("flagSimple"));
      Assert.assertNotNull(doc.field("dateField"));

      loadedJavaObj = database.save(loadedJavaObj);
      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      loadedJavaObj = database.load(id);
      doc = database.getRecordByUserObject(loadedJavaObj, false);
      Assert.assertNotNull(doc.field("text"));
      Assert.assertNotNull(doc.field("enumeration"));
      Assert.assertNotNull(doc.field("numberSimple"));
      Assert.assertNotNull(doc.field("longSimple"));
      Assert.assertNotNull(doc.field("doubleSimple"));
      Assert.assertNotNull(doc.field("floatSimple"));
      Assert.assertNotNull(doc.field("byteSimple"));
      Assert.assertNotNull(doc.field("flagSimple"));
      Assert.assertNotNull(doc.field("dateField"));

      Assert.assertEquals(loadedJavaObj.getText().length, 10);
      Assert.assertEquals(loadedJavaObj.getNumberSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getLongSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getDoubleSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getFloatSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getByteSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getFlagSimple().length, 10);
      Assert.assertEquals(loadedJavaObj.getEnumeration().length, 10);
      Assert.assertEquals(loadedJavaObj.getDateField().length, 10);

      for (int i = 0; i < 10; i++) {
        int j = i + 10;
        Assert.assertEquals(loadedJavaObj.getText()[i], j + "");
        Assert.assertEquals(loadedJavaObj.getNumberSimple()[i], j);
        Assert.assertEquals(loadedJavaObj.getLongSimple()[i], (long) j);
        Assert.assertEquals(loadedJavaObj.getDoubleSimple()[i], (double) j);
        Assert.assertEquals(loadedJavaObj.getFloatSimple()[i], (float) j);
        Assert.assertEquals(loadedJavaObj.getByteSimple()[i], (byte) j);
        Assert.assertEquals(loadedJavaObj.getFlagSimple()[i], (j % 2 == 0));
        EnumTest enumCheck = (j % 2 == 0) ? EnumTest.ENUM2 : ((j % 3 == 0) ? EnumTest.ENUM3 : EnumTest.ENUM1);
        Assert.assertEquals(loadedJavaObj.getEnumeration()[i], enumCheck);
        cal.set(Calendar.DAY_OF_MONTH, (j + 1));
        Assert.assertEquals(loadedJavaObj.getDateField()[i], cal.getTime());
      }

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      loadedJavaObj = database.load(id);
      doc = database.getRecordByUserObject(loadedJavaObj, false);

      Assert.assertTrue(((Collection<?>) doc.field("text")).iterator().next() instanceof String);
      Assert.assertTrue(((Collection<?>) doc.field("enumeration")).iterator().next() instanceof String);
      Assert.assertTrue(((Collection<?>) doc.field("numberSimple")).iterator().next() instanceof Integer);
      Assert.assertTrue(((Collection<?>) doc.field("longSimple")).iterator().next() instanceof Long);
      Assert.assertTrue(((Collection<?>) doc.field("doubleSimple")).iterator().next() instanceof Double);
      Assert.assertTrue(((Collection<?>) doc.field("floatSimple")).iterator().next() instanceof Float);
      Assert.assertTrue(((Collection<?>) doc.field("byteSimple")).iterator().next() instanceof Byte);
      Assert.assertTrue(((Collection<?>) doc.field("flagSimple")).iterator().next() instanceof Boolean);
      Assert.assertTrue(((Collection<?>) doc.field("dateField")).iterator().next() instanceof Date);

      // TODO - remove this delete to test simple type collections JSON import/export
      database.delete(id);
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testSimpleArrayTypes")
  public void collectionsDocumentTypeTestPhaseOne() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

    try {
      JavaComplexTestClass a = database.newInstance(JavaComplexTestClass.class);

      for (int i = 0; i < 3; i++) {
        a.getList().add(new Child());
        a.getSet().add(new Child());
        a.getChildren().put("" + i, new Child());
      }
      a = database.save(a);
      ORID rid = database.getIdentity(a);

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      List<JavaComplexTestClass> agendas = database.query(new OSQLSynchQuery<JavaComplexTestClass>("SELECT FROM " + rid));
      JavaComplexTestClass testLoadedEntity = agendas.get(0);

      ODocument doc = database.getRecordByUserObject(testLoadedEntity, false);

      checkCollectionImplementations(doc);

      testLoadedEntity = database.save(testLoadedEntity);

      database.freeze(false);
      database.release();

      testLoadedEntity = database.reload(testLoadedEntity, "*:-1", true);

      doc = database.getRecordByUserObject(testLoadedEntity, false);

      checkCollectionImplementations(doc);
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "collectionsDocumentTypeTestPhaseOne")
  public void collectionsDocumentTypeTestPhaseTwo() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      JavaComplexTestClass a = database.newInstance(JavaComplexTestClass.class);

      for (int i = 0; i < 10; i++) {
        a.getList().add(new Child());
        a.getSet().add(new Child());
        a.getChildren().put("" + i, new Child());
      }
      a = database.save(a);
      ORID rid = database.getIdentity(a);

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      List<JavaComplexTestClass> agendas = database.query(new OSQLSynchQuery<JavaComplexTestClass>("SELECT FROM " + rid));
      JavaComplexTestClass testLoadedEntity = agendas.get(0);

      ODocument doc = database.getRecordByUserObject(testLoadedEntity, false);
      checkCollectionImplementations(doc);

      testLoadedEntity = database.save(testLoadedEntity);

      database.freeze(false);
      database.release();

      testLoadedEntity = database.reload(testLoadedEntity, "*:-1", true);

      doc = database.getRecordByUserObject(testLoadedEntity, false);

      checkCollectionImplementations(doc);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "collectionsDocumentTypeTestPhaseTwo")
  public void collectionsDocumentTypeTestPhaseThree() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      JavaComplexTestClass a = database.newInstance(JavaComplexTestClass.class);

      for (int i = 0; i < 100; i++) {
        a.getList().add(new Child());
        a.getSet().add(new Child());
        a.getChildren().put("" + i, new Child());
      }
      a = database.save(a);
      ORID rid = database.getIdentity(a);

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      List<JavaComplexTestClass> agendas = database.query(new OSQLSynchQuery<JavaComplexTestClass>("SELECT FROM " + rid));
      JavaComplexTestClass testLoadedEntity = agendas.get(0);

      ODocument doc = database.getRecordByUserObject(testLoadedEntity, false);
      checkCollectionImplementations(doc);

      testLoadedEntity = database.save(testLoadedEntity);

      database.freeze(false);
      database.release();

      testLoadedEntity = database.reload(testLoadedEntity, "*:-1", true);

      doc = database.getRecordByUserObject(testLoadedEntity, false);

      checkCollectionImplementations(doc);

    } finally {
      database.close();
    }
  }

  protected boolean checkCollectionImplementations(ODocument doc) {
    Object collectionObj = doc.field("list");
    boolean validImplementation = (collectionObj instanceof OTrackedList<?>) || (doc.field("list") instanceof ORecordLazyList);
    if (!validImplementation) {
      Assert.fail("Document list implementation " + collectionObj.getClass().getName()
          + " not compatible with current Object Database loading management");
    }
    collectionObj = doc.field("set");
    validImplementation = (collectionObj instanceof OTrackedSet<?>) || (collectionObj instanceof ORecordLazySet)
        || (collectionObj instanceof OMVRBTreeRIDSet);
    if (!validImplementation) {
      Assert.fail("Document set implementation " + collectionObj.getClass().getName()
          + " not compatible with current Object Database management");
    }
    collectionObj = doc.field("children");
    validImplementation = (collectionObj instanceof OTrackedMap<?>) || (collectionObj instanceof ORecordLazyMap);
    if (!validImplementation) {
      Assert.fail("Document map implementation " + collectionObj.getClass().getName()
          + " not compatible with current Object Database management");
    }
    return validImplementation;
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testDateInTransaction() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      JavaSimpleTestClass javaObj = new JavaSimpleTestClass();
      Date date = new Date();
      javaObj.setDateField(date);
      database.begin(TXTYPE.OPTIMISTIC);
      JavaSimpleTestClass dbEntry = database.save(javaObj);
      database.commit();
      database.detachAll(dbEntry, false);
      Assert.assertEquals(dbEntry.getDateField(), date);
      // Close db
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testCreate")
  public void readAndBrowseDescendingAndCheckHoleUtilization() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void synchQueryCollectionsFetch() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "synchQueryCollectionsFetch")
  public void synchQueryCollectionsFetchNoLazyLoad() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "collectionsDocumentTypeTestPhaseThree")
  public void mapEnumAndInternalObjects() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      // BROWSE ALL THE OBJECTS
      for (OUser u : database.browseClass(OUser.class)) {
        u.save();
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapEnumAndInternalObjects")
  public void mapObjectsLinkTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void listObjectsLinkTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "listObjectsLinkTest")
  public void listObjectsIterationTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      Agenda a = database.newInstance(Agenda.class);

      for (int i = 0; i < 10; i++) {
        a.getEvents().add(database.newInstance(Event.class));
      }
      a = database.save(a);
      ORID rid = database.getIdentity(a);

      database.close();

      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      List<Agenda> agendas = database.query(new OSQLSynchQuery<Agenda>("SELECT FROM " + rid));
      Agenda agenda = agendas.get(0);
      for (Event e : agenda.getEvents()) {
        // NO NEED TO DO ANYTHING, JUST NEED TO ITERATE THE LIST
      }

      agenda = database.save(agenda);

      database.freeze(false);
      database.release();

      agenda = database.reload(agenda, "*:-1", true);

      try {
        agenda.getEvents();
        agenda.getEvents().size();
        for (int i = 0; i < agenda.getEvents().size(); i++) {
          Event e = agenda.getEvents().get(i);
          // NO NEED TO DO ANYTHING, JUST NEED TO ITERATE THE LIST
        }
      } catch (ConcurrentModificationException cme) {
        Assert.fail("Error iterating Object list", cme);
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "listObjectsIterationTest")
  public void mapObjectsListEmbeddedTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsListEmbeddedTest")
  public void mapObjectsSetEmbeddedTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsSetEmbeddedTest")
  public void mapObjectsMapEmbeddedTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsNonExistingKeyTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkTwoSaveTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void enumQueryTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "enumQueryTest")
  public void paramQueryTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkUpdateDatabaseNewInstanceTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateDatabaseNewInstanceTest")
  public void mapObjectsLinkUpdateJavaNewInstanceTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateJavaNewInstanceTest")
  public void mapStringTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapStringTest")
  public void setStringTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "setStringTest")
  public void mapStringListTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "mapStringListTest")
  public void mapStringObjectTest() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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
    } finally {
      database.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "mapStringObjectTest")
  public void testNoGenericCollections() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      JavaNoGenericCollectionsTestClass p = database.newInstance(JavaNoGenericCollectionsTestClass.class);
      Child c1 = new Child();
      c1.setName("1");
      Child c2 = new Child();
      c2.setName("2");
      Child c3 = new Child();
      c3.setName("3");
      Child c4 = new Child();
      c4.setName("4");
      p.getList().add(c1);
      p.getList().add(c2);
      p.getList().add(c3);
      p.getList().add(c4);
      p.getSet().add(c1);
      p.getSet().add(c2);
      p.getSet().add(c3);
      p.getSet().add(c4);
      p.getMap().put("1", c1);
      p.getMap().put("2", c2);
      p.getMap().put("3", c3);
      p.getMap().put("4", c4);
      p = database.save(p);
      ORID rid = database.getIdentity(p);
      database.close();
      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      p = database.load(rid);
      Assert.assertEquals(p.getList().size(), 4);
      Assert.assertEquals(p.getSet().size(), 4);
      Assert.assertEquals(p.getMap().size(), 4);
      for (int i = 0; i < 4; i++) {
        Object o = p.getList().get(i);
        Assert.assertTrue(o instanceof Child);
        Assert.assertEquals(((Child) o).getName(), (i + 1) + "");
        o = p.getMap().get((i + 1) + "");
        Assert.assertTrue(o instanceof Child);
        Assert.assertEquals(((Child) o).getName(), (i + 1) + "");
      }
      for (Object o : p.getSet()) {
        Assert.assertTrue(o instanceof Child);
        int nameToInt = Integer.valueOf(((Child) o).getName());
        Assert.assertTrue(nameToInt > 0 && nameToInt < 5);
      }
      JavaSimpleTestClass other = new JavaSimpleTestClass();
      p.getList().add(other);
      p.getSet().add(other);
      p.getMap().put("5", other);
      database.save(p);
      database.close();
      database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
      p = database.load(rid);
      Assert.assertEquals(p.getList().size(), 5);
      Object o = p.getList().get(4);
      Assert.assertTrue(o instanceof JavaSimpleTestClass);
      o = p.getMap().get("5");
      Assert.assertTrue(o instanceof JavaSimpleTestClass);
      boolean hasOther = false;
      for (Object obj : p.getSet()) {
        hasOther = hasOther || (obj instanceof JavaSimpleTestClass);
      }
      Assert.assertTrue(hasOther);
    } finally {
      database.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "testNoGenericCollections")
  public void testNoGenericCollectionsWrongAdding() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    OLogManager.instance().log(this, Level.INFO, "Serialization error test, this will log errors.", null);
    try {
      JavaNoGenericCollectionsTestClass p = database.newInstance(JavaNoGenericCollectionsTestClass.class);
      // OBJECT ADDING
      boolean throwedEx = false;
      try {
        p.getList().add(new Object());
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      throwedEx = false;
      try {
        p.getSet().add(new Object());
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      throwedEx = false;
      try {
        p.getMap().put("1", new Object());
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);

      // JAVA TYPE ADDING
      try {
        p.getList().add(1);
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      throwedEx = false;
      try {
        p.getList().add("asd");
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      throwedEx = false;
      try {
        p.getSet().add(1);
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      throwedEx = false;
      try {
        p.getSet().add("asd");
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      throwedEx = false;

      try {
        p.getMap().put("1", 1);
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      throwedEx = false;
      try {
        p.getMap().put("1", "ASF");
      } catch (Throwable ose) {
        if (ose instanceof ODatabaseException && ose.getCause() instanceof OSerializationException)
          throwedEx = true;
      }
      Assert.assertTrue(throwedEx);
      OLogManager.instance().log(this, Level.INFO, "Serialization error test ended.", null);
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testNoGenericCollectionsWrongAdding")
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
    try {
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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "update")
  public void testUpdate() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testUpdate")
  public void testSaveMultiCircular() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      startRecordNumber = database.countClusterElements("Profile");

      Profile bObama = database.newInstance(Profile.class, "ThePresident", "Barack", "Obama", null);
      bObama.setLocation(database.newInstance(Address.class, "Residence",
          database.newInstance(City.class, database.newInstance(Country.class, "Hawaii"), "Honolulu"), "unknown"));
      bObama.addFollower(database.newInstance(Profile.class, "PresidentSon1", "Malia Ann", "Obama", bObama));
      bObama.addFollower(database.newInstance(Profile.class, "PresidentSon2", "Natasha", "Obama", bObama));

      database.save(bObama);
    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "testSaveMultiCircular")
  public void createLinked() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      long profiles = database.countClass("Profile");

      Profile neo = new Profile("Neo").setValue("test").setLocation(
          new Address("residence", new City(new Country("Spain"), "Madrid"), "Rio de Castilla"));
      neo.addFollowing(new Profile("Morpheus"));
      neo.addFollowing(new Profile("Trinity"));

      database.save(neo);

      Assert.assertEquals(database.countClass("Profile"), profiles + 3);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void browseLinked() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      for (Profile obj : database.browseClass(Profile.class).setFetchPlan("*:1")) {
        if (obj.getNick().equals("Neo")) {
          Assert.assertEquals(obj.getFollowers().size(), 0);
          Assert.assertEquals(obj.getFollowings().size(), 2);
        } else if (obj.getNick().equals("Morpheus") || obj.getNick().equals("Trinity")) {
          Assert.assertEquals(obj.getFollowers().size(), 1);
          Assert.assertEquals(obj.getFollowings().size(), 0);
        }
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void checkLazyLoadingOff() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "checkLazyLoadingOff")
  public void queryPerFloat() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      final List<Account> result = database.query(new OSQLSynchQuery<ODocument>("select * from Account where salary = 500.10"));

      Assert.assertTrue(result.size() > 0);

      Account account;
      for (int i = 0; i < result.size(); ++i) {
        account = result.get(i);

        Assert.assertEquals(account.getSalary(), 500.10f);
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "queryPerFloat")
  public void queryCross3Levels() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      final List<Profile> result = database.query(new OSQLSynchQuery<Profile>(
          "select from Profile where location.city.country.name = 'Spain'"));

      Assert.assertTrue(result.size() > 0);

      Profile profile;
      for (int i = 0; i < result.size(); ++i) {
        profile = result.get(i);

        Assert.assertEquals(profile.getLocation().getCity().getCountry().getName(), "Spain");
      }

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "queryCross3Levels")
  public void deleteFirst() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      database.getMetadata().getSchema().reload();

      startRecordNumber = database.countClusterElements("Account");

      // DELETE ALL THE RECORD IN THE CLUSTER
      for (Object obj : database.browseCluster("Account")) {
        database.delete(obj);
        break;
      }

      Assert.assertEquals(database.countClusterElements("Account"), startRecordNumber - 1);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void commandWithPositionalParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
      List<Profile> result = database.command(query).execute("Barack", "Obama");

      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryWithPositionalParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {
      database.getMetadata().getSchema().reload();

      final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
      List<Profile> result = database.query(query, "Barack", "Obama");

      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryWithRidAsParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      Profile profile = (Profile) database.browseClass("Profile").next();

      final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
      List<Profile> result = database.query(query, new ORecordId(profile.getId()));

      Assert.assertEquals(result.size(), 1);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryWithRidStringAsParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      Profile profile = (Profile) database.browseClass("Profile").next();

      OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
      List<Profile> result = database.query(query, profile.getId());

      Assert.assertEquals(result.size(), 1);

      // TEST WITHOUT # AS PREFIX
      query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
      result = database.query(query, profile.getId().substring(1));

      Assert.assertEquals(result.size(), 1);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void commandWithNamedParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
          "select from Profile where name = :name and surname = :surname");

      HashMap<String, String> params = new HashMap<String, String>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      List<Profile> result = database.command(query).execute(params);
      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked", expectedExceptions = OQueryParsingException.class)
  public void commandWithWrongNamedParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
          "select from Profile where name = :name and surname = :surname%");

      HashMap<String, String> params = new HashMap<String, String>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      List<Profile> result = database.command(query).execute(params);
      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryWithNamedParameters() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
          "select from Profile where name = :name and surname = :surname");

      HashMap<String, String> params = new HashMap<String, String>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      List<Profile> result = database.query(query, params);
      Assert.assertTrue(result.size() != 0);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryWithObjectAsParameter() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryWithListOfObjectAsParameter() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryConcatAttrib() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      database.getMetadata().getSchema().reload();

      Assert
          .assertTrue(database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country'")).size() > 0);
      Assert.assertEquals(
          database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country22'")).size(), 0);

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryPreparredTwice() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void commandPreparredTwice() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @SuppressWarnings("deprecation")
  @Test(dependsOnMethods = "update")
  public void testOldObjectImplementation() {
    ODatabaseObjectTx db = new ODatabaseObjectTx(url).open("admin", "admin");
    try {
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
    } finally {
      db.close();
    }
  }

  @Test(dependsOnMethods = "oidentifableFieldsTest")
  public void testEmbeddedDeletion() throws Exception {
    OObjectDatabaseTx db = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      db.close();
    }
  }

  @Test(dependsOnMethods = "testUpdate")
  public void testEmbeddedBinary() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

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

    } finally {
      database.close();
    }
  }

  @Test(dependsOnMethods = "createLinked")
  public void queryById() {
    database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
    try {

      List<Profile> result1 = database.query(new OSQLSynchQuery<Profile>("select from Profile limit 1"));

      List<Profile> result2 = database.query(new OSQLSynchQuery<Profile>("select from Profile where @rid = ?"), result1.get(0)
          .getId());

      Assert.assertTrue(result2.size() != 0);

    } finally {
      database.close();
    }
  }

}
