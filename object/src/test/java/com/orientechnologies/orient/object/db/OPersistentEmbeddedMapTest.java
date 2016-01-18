package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.object.db.entity.Car;
import com.orientechnologies.orient.object.db.entity.Person;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 16/12/15.
 */
public class OPersistentEmbeddedMapTest {

  private OPartitionedDatabasePool pool;

  @BeforeMethod
  public void setup() {
    final String url = "memory:tmpdb";
    new OObjectDatabaseTx(url).create().close();

    pool = new OPartitionedDatabasePool(url, "admin", "admin");

    OObjectDatabaseTx db = new OObjectDatabaseTx(pool.acquire());
    try {
      db.setAutomaticSchemaGeneration(true);
      OEntityManager entityManager = db.getEntityManager();
      entityManager.registerEntityClass(Car.class);
      entityManager.registerEntityClass(Person.class);

      db.getMetadata().getSchema().synchronizeSchema();
    } finally {
      db.close();
    }
  }

  @AfterMethod
  public void destroy() {
    pool.close();
  }

  @Test
  public void embeddedMapShouldContainCorrectValues() {
    Person person = createTestPerson();
    Person retrievedPerson;
    OObjectDatabaseTx db = new OObjectDatabaseTx(pool.acquire());
    try {
      db.save(person);
      retrievedPerson = db.browseClass(Person.class).next();
      retrievedPerson = db.detachAll(retrievedPerson, true);
    } finally {
      db.close();
    }

    Assert.assertEquals(person, retrievedPerson);
  }

  private Person createTestPerson() {
    Map<String, Car> placeToCar = new HashMap<String, Car>();
    placeToCar.put("USA", new Car("Cadillac Escalade", 1990));
    placeToCar.put("Japan", new Car("Nissan Skyline", 2001));
    placeToCar.put("UK", new Car("Jaguar XR", 2007));

    return new Person("John", placeToCar);
  }


}
