package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public abstract class BaseTest<T extends ODatabase> {

  private static final boolean keepDatabase = Boolean.getBoolean("orientdb.test.keepDatabase");
  private static final Map<String, OrientDB> contexts = new ConcurrentHashMap<>();

  public static String prepareUrl(String url) {
    if (url != null) return url;

    String storageType;
    final String config = System.getProperty("orientdb.test.env");
    if ("ci".equals(config) || "release".equals(config)) storageType = "plocal";
    else storageType = System.getProperty("storageType");

    if (storageType == null) storageType = "memory";

    if ("remote".equals(storageType)) return storageType + ":localhost/demo";
    else {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      return storageType + ":" + buildDirectory + "/test-db/demo";
    }
  }

  protected OrientDB baseContext;
  protected T database;
  protected String url;
  private boolean dropDb = false;
  private String storageType;
  private boolean autoManageDatabase = true;
  protected OURLConnection data;

  protected BaseTest() {}

  @Parameters(value = "url")
  public BaseTest(@Optional String url) {
    String config = System.getProperty("orientdb.test.env");
    if ("ci".equals(config) || "release".equals(config)) storageType = "plocal";
    else storageType = System.getProperty("storageType");

    if (storageType == null) storageType = "memory";

    if (url == null) {
      if ("remote".equals(storageType)) {
        url = storageType + ":localhost/demo";
        dropDb = !keepDatabase;
      } else {
        final String buildDirectory = System.getProperty("buildDirectory", ".");
        url = storageType + ":" + buildDirectory + "/test-db/demo";
        dropDb = !keepDatabase;
      }
    }
    this.url = url;
    this.data = OURLHelper.parse(this.url);
    this.baseContext = getContext(this.data.getType() + ":" + this.data.getPath());
    if (!url.startsWith("remote:")) {
      if (!baseContext.exists(data.getDbName())) {
        createBaseDb();
      }
    }
  }

  protected void createBaseDb() {
    createBaseDb(data.getDbName());
  }

  protected void createBaseDb(String name) {
    String dbType = data.getDbType().map((x) -> x.toString()).orElse("plocal");
    baseContext
        .execute(
            "create database ? "
                + dbType
                + " users (admin identified by 'admin' role admin, writer identified by 'writer'"
                + " role writer, reader identified by 'reader' role reader) ",
            name)
        .close();
  }

  @Parameters(value = "url")
  public BaseTest(@Optional String url, String prefix) {
    String config = System.getProperty("orientdb.test.env");
    if ("ci".equals(config) || "release".equals(config)) storageType = "plocal";
    else storageType = System.getProperty("storageType");

    if (storageType == null) storageType = "memory";

    if (url == null) {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      url = storageType + ":" + buildDirectory + "/test-db/demo" + prefix;
      dropDb = !keepDatabase;
    } else url = url + prefix;

    this.url = url;
    this.data = OURLHelper.parse(this.url);
    this.baseContext = getContext(this.data.getType() + ":" + this.data.getPath());
    if (!url.startsWith("remote:")) {
      if (!baseContext.exists(data.getDbName())) {
        createBaseDb();
      }
    }
  }

  protected abstract T session(String database, String user, String password);

  @BeforeClass
  public void beforeClass() throws Exception {

    if (dropDb) {

      if (baseContext.exists(data.getDbName())) {
        baseContext.drop(data.getDbName());
      }

      createDatabase();
    }
    if (baseContext.exists(data.getDbName())) {
      database = session(data.getDbName(), "admin", "admin");
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (!autoManageDatabase) return;

    if (dropDb) {
      if (baseContext.exists(data.getDbName())) {
        baseContext.drop(data.getDbName());
      }
    } else {
      if (!database.isClosed()) {
        database.activateOnCurrentThread();
        database.close();
      }
    }
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    if (!autoManageDatabase) return;
    if (database == null || database.isClosed()) {
      database = session(data.getDbName(), "admin", "admin");
    }
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (!autoManageDatabase) return;

    if (!database.isClosed()) {
      database.activateOnCurrentThread();
      database.close();
    }
  }

  protected void createDatabase() throws IOException {
    createBaseDb();
  }

  protected static String getTestEnv() {
    return System.getProperty("orientdb.test.env");
  }

  protected final String getStorageType() {
    return storageType;
  }

  protected void createBasicTestSchema() {
    ODatabase database = this.database;
    if (database instanceof OObjectDatabaseTx)
      database = ((OObjectDatabaseTx) database).getUnderlying();

    if (database.getMetadata().getSchema().existsClass("Whiz")) return;

    database.addCluster("csv");
    database.addCluster("flat");
    database.addCluster("binary");

    OClass account = database.getMetadata().getSchema().createClass("Account", 1, (OClass[]) null);
    account.createProperty("id", OType.INTEGER);
    account.createProperty("birthDate", OType.DATE);
    account.createProperty("binary", OType.BINARY);

    database.getMetadata().getSchema().createClass("Company", account);

    OClass profile = database.getMetadata().getSchema().createClass("Profile", 1, (OClass[]) null);
    profile
        .createProperty("nick", OType.STRING)
        .setMin("3")
        .setMax("30")
        .createIndex(OClass.INDEX_TYPE.UNIQUE, new ODocument().field("ignoreNullValues", true));
    profile
        .createProperty("name", OType.STRING)
        .setMin("3")
        .setMax("30")
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    profile.createProperty("surname", OType.STRING).setMin("3").setMax("30");
    profile.createProperty("registeredOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    profile.createProperty("lastAccessOn", OType.DATETIME).setMin("2010-01-01 00:00:00");
    profile.createProperty("photo", OType.TRANSIENT);

    OClass whiz = database.getMetadata().getSchema().createClass("Whiz", 1, (OClass[]) null);
    whiz.createProperty("id", OType.INTEGER);
    whiz.createProperty("account", OType.LINK, account);
    whiz.createProperty("date", OType.DATE).setMin("2010-01-01");
    whiz.createProperty("text", OType.STRING).setMandatory(true).setMin("1").setMax("140");
    whiz.createProperty("replyTo", OType.LINK, account);

    OClass strictTest =
        database.getMetadata().getSchema().createClass("StrictTest", 1, (OClass[]) null);
    strictTest.setStrictMode(true);
    strictTest.createProperty("id", OType.INTEGER).isMandatory();
    strictTest.createProperty("name", OType.STRING);

    OClass animalRace =
        database.getMetadata().getSchema().createClass("AnimalRace", 1, (OClass[]) null);
    animalRace.createProperty("name", OType.STRING);

    OClass animal = database.getMetadata().getSchema().createClass("Animal", 1, (OClass[]) null);
    animal.createProperty("races", OType.LINKSET, animalRace);
    animal.createProperty("name", OType.STRING);
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
    // ONLY PLOCAL AND MEMORY STORAGES SUPPORTED
    return !((ODatabaseDocumentInternal) database).isRemote();
  }

  protected void checkEmbeddedDB() {
    final ODatabaseDocumentInternal db;
    if (database instanceof ODatabaseWrapperAbstract) {
      db = (ODatabaseDocumentInternal) ((ODatabaseWrapperAbstract) database).getUnderlying();
    } else {
      db = (ODatabaseDocumentInternal) database;
    }
    if (db.isRemote()) {
      throw new SkipException("Test is running only in embedded database");
    }
  }

  protected OIndex getIndex(final String indexName) {
    final ODatabaseDocumentInternal db;
    if (database instanceof ODatabaseWrapperAbstract) {
      db = (ODatabaseDocumentInternal) ((ODatabaseWrapperAbstract) database).getUnderlying();
    } else {
      db = (ODatabaseDocumentInternal) database;
    }
    return db.getMetadata().getIndexManagerInternal().getIndex(db, indexName);
  }

  protected Collection<? extends OIndex> getIndexes() {
    final ODatabaseDocumentInternal db;
    if (database instanceof ODatabaseWrapperAbstract) {
      db = (ODatabaseDocumentInternal) ((ODatabaseWrapperAbstract) database).getUnderlying();
    } else {
      db = (ODatabaseDocumentInternal) database;
    }
    return db.getMetadata().getIndexManagerInternal().getIndexes(db);
  }

  public static final OrientDB getContext(String ctx) {
    return contexts.computeIfAbsent(
        ctx,
        (String base) -> {
          return new OrientDB(base, "root", "root", OrientDBConfig.defaultConfig());
        });
  }
}
