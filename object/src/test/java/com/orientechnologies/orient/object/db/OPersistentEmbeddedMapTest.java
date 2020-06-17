package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.object.db.entity.Car;
import com.orientechnologies.orient.object.db.entity.Person;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 16/12/15. */
public class OPersistentEmbeddedMapTest {

  private OPartitionedDatabasePool pool;
  private OObjectDatabaseTx createdDb;

  @Before
  public void setup() {
    final String url = "memory:tmpdb";
    createdDb = new OObjectDatabaseTx(url);
    createdDb.create();

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

  @After
  public void destroy() {
    pool.close();
    createdDb.drop();
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
