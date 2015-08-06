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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineClusterException;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

@Test(groups = "schema")
public class SchemaTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public SchemaTest(@Optional String url) {
    super(url);
  }

  public void createSchema() throws IOException {

    if (database.getMetadata().getSchema().existsClass("Account"))
      return;

    createBasicTestSchema();
  }

  @Test(dependsOnMethods = "createSchema")
  public void checkSchema() {

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

  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkInvalidNames() {

    OSchema schema = database.getMetadata().getSchema();

    try {
      schema.createClass("TestInvalidName:");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

    try {
      schema.createClass("TestInvalidName,");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

    try {
      schema.createClass("TestInvalidName;");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

    try {
      schema.createClass("TestInvalid Name");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

    try {
      schema.createClass("TestInvalid%Name:");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

    try {
      schema.createClass("TestInvalid@Name:");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

    try {
      schema.createClass("TestInvalid=Name:");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

    try {
      schema.createClass("TestInvalid.Name");
      Assert.assertTrue(false);
    } catch (OSchemaException e) {
    }

  }

  @Test(dependsOnMethods = "checkSchema")
  public void checkSchemaApi() {

    OSchema schema = database.getMetadata().getSchema();

    try {
      Assert.assertNull(schema.getClass("Animal33"));
    } catch (OSchemaException e) {
    }

  }

  @Test(dependsOnMethods = "checkSchemaApi")
  public void checkClusters() {

    for (OClass cls : database.getMetadata().getSchema().getClasses()) {
      if (!cls.isAbstract())
        assert database.getClusterNameById(cls.getDefaultClusterId()) != null;
    }

  }

  @Test(dependsOnMethods = "createSchema")
  public void checkDatabaseSize() {

    Assert.assertTrue(database.getSize() > 0);

  }

  @Test(dependsOnMethods = "createSchema")
  public void checkTotalRecords() {

    Assert.assertTrue(database.getStorage().countRecords() > 0);

  }

  @Test(expectedExceptions = OValidationException.class)
  public void checkErrorOnUserNoPasswd() {

    database.getMetadata().getSecurity().createUser("error", null, (String) null);

  }

  @Test
  public void testMultiThreadSchemaCreation() throws InterruptedException {

    Thread thread = new Thread(new Runnable() {

      @Override
      public void run() {
        ODatabaseRecordThreadLocal.INSTANCE.set(database);
        ODocument doc = new ODocument("NewClass");
        database.save(doc);

        doc.delete();
        database.getMetadata().getSchema().dropClass("NewClass");

      }
    });

    thread.start();
    thread.join();
  }

  @Test
  public void createAndDropClassTestApi() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    OClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));

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

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));

  }

  @Test
  public void createAndDropClassTestCommand() {

    final String testClassName = "dropTestClass";
    final int clusterId;
    OClass dropTestClass = database.getMetadata().getSchema().createClass(testClassName);
    clusterId = dropTestClass.getDefaultClusterId();
    database.getMetadata().getSchema().reload();
    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNotNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), clusterId);
    Assert.assertNotNull(database.getClusterNameById(clusterId));

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

    dropTestClass = database.getMetadata().getSchema().getClass(testClassName);
    Assert.assertNull(dropTestClass);
    Assert.assertEquals(database.getStorage().getClusterIdByName(testClassName), -1);
    Assert.assertNull(database.getClusterNameById(clusterId));

  }

  @Test(dependsOnMethods = "createSchema")
  public void customAttributes() {

    // TEST CUSTOM PROPERTY CREATION
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").setCustom("stereotype", "icon");

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"), "icon");

    // TEST CUSTOM PROPERTY EXISTS EVEN AFTER REOPEN

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"), "icon");

    // TEST CUSTOM PROPERTY REMOVAL
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").setCustom("stereotype", null);
    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"), null);

    // TEST CUSTOM PROPERTY UPDATE
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").setCustom("stereotype", "polygon");
    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY UDPATED EVEN AFTER REOPEN

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("stereotype"),
        "polygon");

    // TEST CUSTOM PROPERTY WITH =

    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").setCustom("equal", "this = that");

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("equal"),
        "this = that");

    // TEST CUSTOM PROPERTY WITH = AFTER REOPEN

    Assert.assertEquals(database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getCustom("equal"),
        "this = that");

  }

  @Test(dependsOnMethods = "createSchema")
  public void alterAttributes() {

    OClass company = database.getMetadata().getSchema().getClass("Company");
    OClass superClass = company.getSuperClass();

    Assert.assertNotNull(superClass);
    boolean found = false;
    for (OClass c : superClass.getSubclasses()) {
      if (c.equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertEquals(found, true);

    company.setSuperClass(null);
    Assert.assertNull(company.getSuperClass());
    for (OClass c : superClass.getSubclasses()) {
      Assert.assertNotSame(c, company);
    }

    database.command(new OCommandSQL("alter class " + company.getName() + " superclass " + superClass.getName())).execute();

    database.getMetadata().getSchema().reload();
    company = database.getMetadata().getSchema().getClass("Company");
    superClass = company.getSuperClass();

    Assert.assertNotNull(company.getSuperClass());
    found = false;
    for (OClass c : superClass.getSubclasses()) {
      if (c.equals(company)) {
        found = true;
        break;
      }
    }
    Assert.assertEquals(found, true);

  }

  @Test
  public void invalidClusterWrongClusterId() {

    try {
      database.command(new OCommandSQL("create class Antani cluster 212121")).execute();
      Assert.fail();
    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OCommandSQLParsingException);
    } finally {

    }
  }

  @Test
  public void invalidClusterWrongClusterName() {

    try {
      database.command(new OCommandSQL("create class Antani cluster blaaa")).execute();
      Assert.fail();

    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OCommandSQLParsingException);
    } finally {

    }
  }

  @Test
  public void invalidClusterWrongKeywords() {

    try {
      database.command(new OCommandSQL("create class Antani the pen is on the table")).execute();
      Assert.fail();
    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OCommandSQLParsingException);
    } finally {

    }
  }

  @Test
  public void testRenameClass() {

    OClass oClass = database.getMetadata().getSchema().createClass("RenameClassTest");

    ODocument document = new ODocument("RenameClassTest");
    document.save();

    document.reset();

    document.setClassName("RenameClassTest");
    document.save();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from RenameClassTest"));
    Assert.assertEquals(result.size(), 2);

    oClass.set(OClass.ATTRIBUTES.NAME, "RenameClassTest2");

    database.getLocalCache().clear();

    result = database.query(new OSQLSynchQuery<ODocument>("select from RenameClassTest2"));
    Assert.assertEquals(result.size(), 2);

  }

  public void testMinimumClustersAndClusterSelection() {

    database.command(new OCommandSQL("alter database minimumclusters 3")).execute();

    try {
      database.command(new OCommandSQL("create class multipleclusters")).execute();

      database.reload();

      Assert.assertFalse(database.existsCluster("multipleclusters"));

      for (int i = 0; i < 3; ++i) {
        Assert.assertTrue(database.existsCluster("multipleclusters_" + i));
      }

      for (int i = 0; i < 6; ++i) {
        new ODocument("multipleclusters").field("num", i).save();
      }

      // CHECK THERE ARE 2 RECORDS IN EACH CLUSTER (ROUND-ROBIN STRATEGY)
      for (int i = 0; i < 3; ++i) {
        Assert.assertEquals(database.countClusterElements(database.getClusterIdByName("multipleclusters_" + i)), 2);
      }

      // DELETE ALL THE RECORDS
      int deleted = database.command(new OCommandSQL("delete from cluster:multipleclusters_2")).execute();
      Assert.assertEquals(deleted, 2);

      // CHANGE CLASS STRATEGY to BALANCED
      database.command(new OCommandSQL("alter class multipleclusters clusterselection balanced")).execute();
      database.reload();
      database.getMetadata().getSchema().reload();

      for (int i = 0; i < 2; ++i) {
        new ODocument("multipleclusters").field("num", i).save();
      }

      Assert.assertEquals(database.countClusterElements(database.getClusterIdByName("multipleclusters_2")), 2);

    } finally {
      // RESTORE DEFAULT
      database.command(new OCommandSQL("alter database minimumclusters 1")).execute();

    }
  }

  public void testExchangeCluster() {

    try {
      database.command(new OCommandSQL("CREATE CLASS TestRenameClusterOriginal")).execute();

      swapClusters(database, 1);
      swapClusters(database, 2);
      swapClusters(database, 3);
    } finally {

    }
  }

  public void testOfflineCluster() {
    database.command(new OCommandSQL("create class TestOffline")).execute();
    database.command(new OCommandSQL("insert into TestOffline set status = 'offline'")).execute();

    List<OIdentifiable> result = database.command(new OCommandSQL("select from TestOffline")).execute();
    Assert.assertNotNull(result);
    Assert.assertFalse(result.isEmpty());

    ODocument record = result.get(0).getRecord();

    // TEST NO EFFECTS
    Boolean changed = database.command(new OCommandSQL("alter cluster TestOffline status online")).execute();
    Assert.assertFalse(changed);

    // PUT IT OFFLINE
    changed = database.command(new OCommandSQL("alter cluster TestOffline status offline")).execute();
    Assert.assertTrue(changed);

    // NO DATA?
    result = database.command(new OCommandSQL("select from TestOffline")).execute();
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());

    // TEST NO EFFECTS
    changed = database.command(new OCommandSQL("alter cluster TestOffline status offline")).execute();
    Assert.assertFalse(changed);

    // TEST SAVING OF OFFLINE STATUS

    // TEST UPDATE - NO EFFECT
    Assert.assertEquals(database.command(new OCommandSQL("update TestOffline set name = 'yeah'")).execute(), 0);

    // TEST DELETE - NO EFFECT
    Assert.assertEquals(database.command(new OCommandSQL("delete from TestOffline")).execute(), 0);

    // TEST CREATE -> EXCEPTION
    try {
      Object res = database.command(
          new OCommandSQL("insert into TestOffline set name = 'offline', password = 'offline', status = 'ACTIVE'")).execute();
      Assert.assertTrue(false);
    } catch (OOfflineClusterException e) {
      Assert.assertTrue(true);
    }

    // TEST UDPATE RECORD -> EXCEPTION
    try {
      record.field("status", "offline").save();
      Assert.assertTrue(false);
    } catch (OOfflineClusterException e) {
      Assert.assertTrue(true);
    }

    // TEST DELETE RECORD -> EXCEPTION
    try {
      record.delete();
      Assert.assertTrue(false);
    } catch (OOfflineClusterException e) {
      Assert.assertTrue(true);
    }

    // TEST DELETE RECORD -> EXCEPTION
    try {
      record.reload(null, true);
      Assert.assertTrue(false);
    } catch (OOfflineClusterException e) {
      Assert.assertTrue(true);
    }

    // RESTORE IT ONLINE
    changed = database.command(new OCommandSQL("alter cluster TestOffline status online")).execute();
    Assert.assertTrue(changed);

    result = database.command(new OCommandSQL("select from TestOffline")).execute();
    Assert.assertNotNull(result);
    Assert.assertFalse(result.isEmpty());

  }

  public void testExistsProperty() {
    OSchema schema = database.getMetadata().getSchema();
    OClass classA = schema.createClass("TestExistsA");
    classA.createProperty("property", OType.STRING);
    Assert.assertTrue(classA.existsProperty("property"));
    Assert.assertNotNull(classA.getProperty("property"));
    OClass classB = schema.createClass("TestExistsB", classA);

    Assert.assertNotNull(classB.getProperty("property"));
    Assert.assertTrue(classB.existsProperty("property"));

    schema = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot();
    classB = schema.getClass("TestExistsB");

    Assert.assertNotNull(classB.getProperty("property"));
    Assert.assertTrue(classB.existsProperty("property"));

  }

  public void testWrongClassNameWithAt() {
    try {
      database.command(new OCommandSQL("create class Ant@ni")).execute();
      Assert.fail();

    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OSchemaException);
    }
  }

  public void testWrongClassNameWithSpace() {
    try {
      database.getMetadata().getSchema().createClass("Anta ni");
      Assert.fail();

    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OSchemaException);
    }
  }

  public void testWrongClassNameWithPercent() {
    try {
      database.command(new OCommandSQL("create class Ant%ni")).execute();
      Assert.fail();

    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OSchemaException);
    }
  }

  public void testWrongClassNameWithComma() {
    try {
      database.getMetadata().getSchema().createClass("Anta,ni");
      Assert.fail();

    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OSchemaException);
    }
  }

  public void testWrongClassNameWithColon() {
    try {
      database.command(new OCommandSQL("create class Ant:ni")).execute();
      Assert.fail();

    } catch (Exception e) {
      if (e instanceof OResponseProcessingException)
        e = (Exception) e.getCause();
      Assert.assertTrue(e instanceof OSchemaException);
    }
  }

  public void testRenameWithSameNameIsNop() {
    database.getMetadata().getSchema().getClass("V").setName("V");
  }

  public void testRenameWithExistentName() {
    try {
      database.getMetadata().getSchema().getClass("V").setName("OUser");
      Assert.fail();
    } catch (OSchemaException e) {
    } catch (OCommandExecutionException e) {
    }
  }

  public void testShortNameAlreadyExists() {
    try {
      database.getMetadata().getSchema().getClass("V").setShortName("OUser");
      Assert.fail();
    } catch (IllegalArgumentException e) {
    } catch (OCommandExecutionException e) {
    }
  }

  private void swapClusters(ODatabaseDocumentTx databaseDocumentTx, int i) {
    databaseDocumentTx.command(new OCommandSQL("CREATE CLASS TestRenameClusterNew extends TestRenameClusterOriginal")).execute();

    databaseDocumentTx.command(new OCommandSQL("INSERT INTO TestRenameClusterNew (iteration) VALUES(" + i + ")")).execute();

    databaseDocumentTx.command(new OCommandSQL("ALTER CLASS TestRenameClusterOriginal removecluster TestRenameClusterOriginal"))
        .execute();
    databaseDocumentTx.command(new OCommandSQL("ALTER CLASS TestRenameClusterNew removecluster TestRenameClusterNew")).execute();
    databaseDocumentTx.command(new OCommandSQL("DROP CLASS TestRenameClusterNew")).execute();
    databaseDocumentTx.command(new OCommandSQL("ALTER CLASS TestRenameClusterOriginal addcluster TestRenameClusterNew")).execute();
    databaseDocumentTx.command(new OCommandSQL("DROP CLUSTER TestRenameClusterOriginal")).execute();
    databaseDocumentTx.command(new OCommandSQL("ALTER CLUSTER TestRenameClusterNew name TestRenameClusterOriginal")).execute();

    databaseDocumentTx.getLocalCache().clear();

    List<ODocument> result = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select * from TestRenameClusterOriginal"));
    Assert.assertEquals(result.size(), 1);

    ODocument document = result.get(0);
    Assert.assertEquals(document.field("iteration"), i);
  }

}
