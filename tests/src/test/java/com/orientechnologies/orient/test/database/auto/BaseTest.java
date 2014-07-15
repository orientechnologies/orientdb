package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.testng.annotations.*;

@Test
public abstract class BaseTest<T extends ODatabaseComplex> {
  protected T      database;
  protected String url;
  private boolean  dropDb = false;
  private String   storageType;

  protected BaseTest() {
  }

  @Parameters(value = "url")
  public BaseTest(@Optional String url) {
    storageType = System.getProperty("storageType");

    if (storageType == null)
      storageType = "memory";

    if (url == null) {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      url = getStorageType() + ":" + buildDirectory + "/test-db/demo";
      dropDb = true;
    }

    database = createDatabaseInstance(url);
    this.url = database.getURL();
  }

  @Parameters(value = "url")
  public BaseTest(@Optional String url, String prefix) {
    if (url == null) {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      url = getStorageType() + ":" + buildDirectory + "/test-db/demo" + prefix;
      dropDb = true;
    } else
      url = url + prefix;

    database = createDatabaseInstance(url);
    this.url = database.getURL();
  }

  protected abstract T createDatabaseInstance(String url);

  @BeforeClass
  public void beforeClass() throws Exception {
    if (dropDb) {
      if (database.exists()) {
        database.open("admin", "admin");
        database.drop();
      }

      createDatabase();
    } else
      database.open("admin", "admin");
  }

  protected void createDatabase() {
    database.create();
  }

  protected final String getStorageType() {
    return storageType;
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (dropDb) {
      if (database.isClosed())
        database.open("admin", "admin");

      database.drop();
    } else {
      if (!database.isClosed())
        database.close();
    }
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    if (database.isClosed())
      database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (!database.isClosed())
      database.close();
  }

  protected void createBasicTestSchema() {
    ODatabaseComplex database = this.database;
    if (database instanceof OObjectDatabaseTx)
      database = ((OObjectDatabaseTx) database).getUnderlying();

    if (database.getMetadata().getSchema().existsClass("Whiz"))
      return;

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
  }
}
