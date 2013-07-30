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

import java.io.IOException;
import java.util.Iterator;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.storage.OStorage;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "schema")
public class SchemaTest {
  private ODatabaseFlat database;
  private String        url;

  @Parameters(value = "url")
  public SchemaTest(String iURL) {
    url = iURL;
  }

  public void createSchema() throws IOException {
    database = new ODatabaseFlat(url);
    if (ODatabaseHelper.existsDatabase(database, "plocal"))
      database.open("admin", "admin");
    else
      database.create();

    if (database.getMetadata().getSchema().existsClass("Account"))
      return;

    Assert.assertNotNull(database.getMetadata().getSchema().getClass("ORIDs"));

    database.addCluster("csv", OStorage.CLUSTER_TYPE.PHYSICAL);
    database.addCluster("flat", OStorage.CLUSTER_TYPE.PHYSICAL);
    database.addCluster("binary", OStorage.CLUSTER_TYPE.PHYSICAL);

    OClass account = database.getMetadata().getSchema()
        .createClass("Account", database.addCluster("account", OStorage.CLUSTER_TYPE.PHYSICAL));
    account.createProperty("id", OType.INTEGER);
    account.createProperty("birthDate", OType.DATE);
    account.createProperty("binary", OType.BINARY);

    database.getMetadata().getSchema().createClass("Company", account);

    OClass profile = database.getMetadata().getSchema()
        .createClass("Profile", database.addCluster("profile", OStorage.CLUSTER_TYPE.PHYSICAL));
    profile.createProperty("nick", OType.STRING).setMin("3").setMax("30").createIndex(OClass.INDEX_TYPE.UNIQUE);
    profile.createProperty("name", OType.STRING).setMin("3").setMax("30").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    profile.createProperty("surname", OType.STRING).setMin("3").setMax("30");
    profile.createProperty("registeredOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    profile.createProperty("lastAccessOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    profile.createProperty("photo", OType.TRANSIENT);

    OClass whiz = database.getMetadata().getSchema().createClass("Whiz");
    whiz.createProperty("id", OType.INTEGER);
    whiz.createProperty("account", OType.LINK, account);
    whiz.createProperty("date", OType.DATE).setMin("2010-01-01");
    whiz.createProperty("text", OType.STRING).setMandatory(true).setMin("1").setMax("140").createIndex(OClass.INDEX_TYPE.FULLTEXT);
    whiz.createProperty("replyTo", OType.LINK, account);

    OClass strictTest = database.getMetadata().getSchema().createClass("StrictTest");
    strictTest.setStrictMode(true);
    strictTest.createProperty("id", OType.INTEGER).isMandatory();
    strictTest.createProperty("name", OType.STRING);

    OClass animalRace = database.getMetadata().getSchema().createClass("AnimalRace");
    animalRace.createProperty("name", OType.STRING);

    OClass animal = database.getMetadata().getSchema().createClass("Animal");
    animal.createProperty("races", OType.LINKSET, animalRace);
    animal.createProperty("name", OType.STRING);

    database.close();
  }

  @Test(dependsOnMethods = "createSchema")
  public void checkSchema() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    OSchema schema = database.getMetadata().getSchema();

    assert schema != null;
    assert schema.getClass("Profile") != null;
    assert schema.getClass("Profile").getProperty("nick").getType() == OType.STRING;
    assert schema.getClass("Profile").getProperty("name").getType() == OType.STRING;
    assert schema.getClass("Profile").getProperty("surname").getType() == OType.STRING;
    assert schema.getClass("Profile").getProperty("registeredOn").getType() == OType.DATETIME;
    assert schema.getClass("Profile").getProperty("lastAccessOn").getType() == OType.DATETIME;

    assert schema.getClass("Whiz") != null;
    assert schema.getClass("whiz").getProperty("account").getType() == OType.LINK;
    assert schema.getClass("whiz").getProperty("account").getLinkedClass().getName().equalsIgnoreCase("Account");
    assert schema.getClass("WHIZ").getProperty("date").getType() == OType.DATE;
    assert schema.getClass("WHIZ").getProperty("text").getType() == OType.STRING;
    assert schema.getClass("WHIZ").getProperty("text").isMandatory();
    assert schema.getClass("WHIZ").getProperty("text").getMin().equals("1");
    assert schema.getClass("WHIZ").getProperty("text").getMax().equals("140");
    assert schema.getClass("whiz").getProperty("replyTo").getType() == OType.LINK;
    assert schema.getClass("Whiz").getProperty("replyTo").getLinkedClass().getName().equalsIgnoreCase("Account");

    database.close();
  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkSchemaApi() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    OSchema schema = database.getMetadata().getSchema();

    try {
      Assert.assertNull(schema.getClass("Animal33"));
    } catch (OSchemaException e) {
    }

    database.close();
  }

  @Test(dependsOnMethods = "checkSchemaApi")
  public void checkClusters() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    for (OClass cls : database.getMetadata().getSchema().getClasses()) {
      if (!cls.isAbstract())
        assert database.getClusterNameById(cls.getDefaultClusterId()) != null;
    }

    database.close();
  }

  @Test(dependsOnMethods = "createSchema")
  public void checkDatabaseSize() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    Assert.assertTrue(database.getSize() > 0);

    database.close();
  }

  @Test(dependsOnMethods = "createSchema")
  public void checkTotalRecords() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    Assert.assertTrue(database.getStorage().countRecords() > 0);

    database.close();
  }

  @Test(expectedExceptions = OValidationException.class)
  public void checkErrorOnUserNoPasswd() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    database.getMetadata().getSecurity().createUser("error", null, (String) null);

    database.close();
  }

  @Test
  public void testMultiThreadSchemaCreation() throws InterruptedException {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    Thread thread = new Thread(new Runnable() {

      public void run() {
        ODatabaseRecordThreadLocal.INSTANCE.set(database);
        ODocument doc = new ODocument("NewClass");
        database.save(doc);

        doc.delete();
        database.getMetadata().getSchema().dropClass("NewClass");

        database.close();
      }
    });

    thread.start();
    thread.join();
  }

  @Test
  public void createAndDropClassTestApi() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");
    final String testClassName = "dropTestClass";
    final int clusterId;
    OClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.close();
    database = null;
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.getMetadata().getSchema().dropClass(testClassName);
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
    database.close();
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
    database.close();
  }

  @Test
  public void createAndDropClassTestCommand() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");
    final String testClassName = "dropTestClass";
    final int clusterId;
    OClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.close();
    database = null;
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));
    database.command(new OCommandSQL("drop class " + testClassName)).execute();
    database.reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
    database.close();
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));
    database.close();
  }

  @Test(dependsOnMethods = "createSchema")
  public void customAttributes() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    // TEST CUSTOM PROPERTY CREATION
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").setCustom("stereotype", "icon");

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"), "icon");

    // TEST CUSTOM PROPERTY EXISTS EVEN AFTER REOPEN
    database.close();
    database.open("admin", "admin");

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"), "icon");

    // TEST CUSTOM PROPERTY REMOVAL
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").setCustom("stereotype", null);
    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"), null);

    // TEST CUSTOM PROPERTY UPDATE
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").setCustom("stereotype", "polygon");
    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY UDPATED EVEN AFTER REOPEN
    database.close();
    database.open("admin", "admin");

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"),
        "polygon");

    database.close();
  }

  @Test(dependsOnMethods = "createSchema")
  public void alterAttributes() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    OClass company = database.getMetadata().getSchema().getClass("Company");
    OClass superClass = company.getSuperClass();

    Assert.assertNotNull(superClass);
    boolean found = false;
    for (Iterator<OClass> it = superClass.getBaseClasses(); it.hasNext();) {
      if (it.next().equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertEquals(found, true);

    company.setSuperClass(null);
    Assert.assertNull(company.getSuperClass());
    for (Iterator<OClass> it = superClass.getBaseClasses(); it.hasNext();) {
      Assert.assertNotSame(it.next(), company);
    }

    database.command(new OCommandSQL("alter class " + company.getName() + " superclass " + superClass.getName())).execute();

    database.getMetadata().getSchema().reload();
    company = database.getMetadata().getSchema().getClass("Company");
    superClass = company.getSuperClass();

    Assert.assertNotNull(company.getSuperClass());
    found = false;
    for (Iterator<OClass> it = superClass.getBaseClasses(); it.hasNext();) {
      if (it.next().equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertEquals(found, true);

    database.close();

  }

  @Test(expectedExceptions = OCommandSQLParsingException.class)
  public void invalidClusterWrongClusterId() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");
    try {
      database.command(new OCommandSQL("create class Antani cluster 212121")).execute();

    } finally {
      database.close();
    }
  }

  @Test(expectedExceptions = OCommandSQLParsingException.class)
  public void invalidClusterWrongClusterName() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    try {
      database.command(new OCommandSQL("create class Antani cluster blaaa")).execute();
    } finally {
      database.close();
    }
  }

  @Test(expectedExceptions = OCommandSQLParsingException.class)
  public void invalidClusterWrongKeywords() {
    database = new ODatabaseFlat(url);
    database.open("admin", "admin");

    try {
      database.command(new OCommandSQL("create class Antani the pen is on the table")).execute();
    } finally {
      database.close();
    }
  }
}
