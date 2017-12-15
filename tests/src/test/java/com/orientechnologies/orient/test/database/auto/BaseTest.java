package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.testng.annotations.*;

import java.io.IOException;

@Test
public abstract class BaseTest<T extends ODatabase> {

  private static boolean keepDatabase = Boolean.getBoolean("orientdb.test.keepDatabase");

  public static String prepareUrl(String url) {
    if (url != null)
      return url;

    String storageType;
    final String config = System.getProperty("orientdb.test.env");
    if ("ci".equals(config) || "release".equals(config))
      storageType = "plocal";
    else
      storageType = System.getProperty("storageType");

    if (storageType == null)
      storageType = "memory";

    if ("remote".equals(storageType))
      return storageType + ":localhost/demo";
    else {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      return storageType + ":" + buildDirectory + "/test-db/demo";
    }
  }

  protected T      database;
  protected String url;
  private boolean dropDb = false;
  private String storageType;
  private boolean autoManageDatabase = true;

  protected BaseTest() {
  }

  @Parameters(value = "url")
  public BaseTest(@Optional String url) {
    String config = System.getProperty("orientdb.test.env");
    if ("ci".equals(config) || "release".equals(config))
      storageType = "plocal";
    else
      storageType = System.getProperty("storageType");

    if (storageType == null)
      storageType = "memory";

    if (url == null) {
      if ("remote".equals(storageType)) {
        url = getStorageType() + ":localhost/demo";
        dropDb = !keepDatabase;
      } else {
        final String buildDirectory = System.getProperty("buildDirectory", ".");
        url = getStorageType() + ":" + buildDirectory + "/test-db/demo";
        dropDb = !keepDatabase;
      }
    }

    if (!url.startsWith("remote:")) {
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
      if (!db.exists())
        db.create().close();
    }

    this.url = url;
  }

  @Parameters(value = "url")
  public BaseTest(@Optional String url, String prefix) {
    String config = System.getProperty("orientdb.test.env");
    if ("ci".equals(config) || "release".equals(config))
      storageType = "plocal";
    else
      storageType = System.getProperty("storageType");

    if (storageType == null)
      storageType = "memory";

    if (url == null) {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      url = getStorageType() + ":" + buildDirectory + "/test-db/demo" + prefix;
      dropDb = !keepDatabase;
    } else
      url = url + prefix;

    if (!url.startsWith("remote:")) {
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
      if (!db.exists())
        db.create().close();
    }

    this.url = url;
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    database = createDatabaseInstance(url);
    this.url = database.getURL();

    String remoteStorageType = storageType;

    if (dropDb) {
      if (storageType.equals("remote"))
        remoteStorageType = "plocal";

      if (ODatabaseHelper.existsDatabase(database, remoteStorageType)) {
        ODatabaseHelper.openDatabase(database);
        ODatabaseHelper.dropDatabase(database, remoteStorageType);
      }

      createDatabase();
    }

    ODatabaseHelper.openDatabase(database);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (!autoManageDatabase)
      return;

    if (dropDb) {
      if (database.isClosed())
        ODatabaseHelper.openDatabase(database);

      String remoteStorageType = storageType;
      if (storageType.equals("remote"))
        remoteStorageType = "plocal";

      ODatabaseHelper.dropDatabase(database, remoteStorageType);
    } else {
      if (!database.isClosed()) {
        database.activateOnCurrentThread();
        database.close();
      }
    }
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    if (!autoManageDatabase)
      return;
    database.activateOnCurrentThread();
    if (database.isClosed())
      ODatabaseHelper.openDatabase(database);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (!autoManageDatabase)
      return;

    if (!database.isClosed()) {
      database.activateOnCurrentThread();
      database.close();
    }
  }

  protected abstract T createDatabaseInstance(String url);

  protected void createDatabase() throws IOException {
    ODatabaseHelper.createDatabase(database, database.getURL());
  }

  protected String getTestEnv() {
    return System.getProperty("orientdb.test.env");
  }

  protected final String getStorageType() {
    return storageType;
  }

  protected void createBasicTestSchema() {
    ODatabase database = this.database;
    if (database instanceof OObjectDatabaseTx)
      database = ((OObjectDatabaseTx) database).getUnderlying();

    if (database.getMetadata().getSchema().existsClass("Whiz"))
      return;

    if (!database.existsCluster("csv")) {
      database.addCluster("csv");
    }
    if (!database.existsCluster("flat")) {
      database.addCluster("flat");
    }
    if (!database.existsCluster("binary")) {
      database.addCluster("binary");
    }

//    database.addBlobCluster("blobCluster");

    final OSchema schema = database.getMetadata().getSchema();
    OClass account;
    if (!schema.existsClass("Account")) {
      account = schema.createClass("Account", 1);
    } else {
      account = schema.getClass("Account");
    }

    if (!account.existsProperty("id")) {
      account.createProperty("id", OType.INTEGER);
    }

    if (!account.existsProperty("birthDate")) {
      account.createProperty("birthDate", OType.DATE);
    }

    if (!account.existsProperty("binary")) {
      account.createProperty("binary", OType.BINARY);
    }

    if (!schema.existsClass("Company")) {
      schema.createClass("Company", account);
    }

    OClass profile;
    if (schema.existsClass("Profile")) {
      profile = schema.getClass("Profile");
    } else {
      profile = schema.createClass("Profile", 1);
    }

    if (!profile.existsProperty("nick")) {
      profile.createProperty("nick", OType.STRING).setMin("3").setMax("30")
          .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));
    }

    if (!profile.existsProperty("name")) {
      profile.createProperty("name", OType.STRING).setMin("3").setMax("30").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    }

    if (!profile.existsProperty("surname")) {
      profile.createProperty("surname", OType.STRING).setMin("3").setMax("30");
    }

    if (!profile.existsProperty("registeredOn")) {
      profile.createProperty("registeredOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    }

    if (!profile.existsProperty("lastAccessOn")) {
      profile.createProperty("lastAccessOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    }

    if (!profile.existsProperty("photo")) {
      profile.createProperty("photo", OType.TRANSIENT);
    }

    OClass whiz;

    if (schema.existsClass("Whiz")) {
      whiz = schema.getClass("Whiz");
    } else {
      whiz = schema.createClass("Whiz", 1);
    }

    if (!whiz.existsProperty("id")) {
      whiz.createProperty("id", OType.INTEGER);
    }

    if (!whiz.existsProperty("account")) {
      whiz.createProperty("account", OType.LINK, account);
    }

    if (!whiz.existsProperty("date")) {
      whiz.createProperty("date", OType.DATE).setMin("2010-01-01");
    }

    if (!whiz.existsProperty("text")) {
      whiz.createProperty("text", OType.STRING).setMandatory(true).setMin("1").setMax("140")
          .createIndex(OClass.INDEX_TYPE.FULLTEXT);
    }

    if (!whiz.existsProperty("replyTo")) {
      whiz.createProperty("replyTo", OType.LINK, account);
    }

    OClass strictTest;
    if (schema.existsClass("StrictTest")) {
      strictTest = schema.getClass("StrictTest");
    } else {
      strictTest = schema.createClass("StrictTest", 1);
    }
    strictTest.setStrictMode(true);

    if (!strictTest.existsProperty("id")) {
      strictTest.createProperty("id", OType.INTEGER).isMandatory();
    }
    if (!strictTest.existsProperty("name")) {
      strictTest.createProperty("name", OType.STRING);
    }

    OClass animalRace;
    if (schema.existsClass("AnimalRace")) {
      animalRace = schema.getClass("AnimalRace");
    } else {
      animalRace = schema.createClass("AnimalRace", 1);
    }

    if (!animalRace.existsProperty("name")) {
      animalRace.createProperty("name", OType.STRING);
    }

    OClass animal;
    if (schema.existsClass("Animal")) {
      animal = schema.getClass("Animal");
    } else {
      animal = schema.createClass("Animal", 1);
    }

    if (!animal.existsProperty("races")) {
      animal.createProperty("races", OType.LINKSET, animalRace);
    }

    if (!animal.existsProperty("name")) {
      animal.createProperty("name", OType.STRING);
    }
  }

  protected boolean isAutoManageDatabase() {
    return autoManageDatabase;
  }

  protected void setAutoManageDatabase(final boolean autoManageDatabase) {
    this.autoManageDatabase = autoManageDatabase;
  }

  protected boolean isDropDb() {
    return dropDb;
  }

  protected void setDropDb(final boolean dropDb) {
    this.dropDb = !keepDatabase && dropDb;
  }

  protected boolean skipTestIfRemote() {
    final OStorage stg = ((ODatabaseDocumentTx) database).getStorage().getUnderlying();

    if (!(stg instanceof OAbstractPaginatedStorage))
      // ONLY PLOCAL AND MEMORY STORAGES SUPPORTED
      return true;
    return false;
  }
}
