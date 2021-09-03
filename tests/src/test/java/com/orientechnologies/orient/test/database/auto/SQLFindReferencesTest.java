/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-findReferences")
public class SQLFindReferencesTest extends DocumentDBBaseTest {

  private static final String WORKPLACE = "Workplace";
  private static final String WORKER = "Worker";
  private static final String CAR = "Car";

  private ORID carID;
  private ORID johnDoeID;
  private ORID janeDoeID;
  private ORID chuckNorrisID;
  private ORID jackBauerID;
  private ORID ctuID;
  private ORID fbiID;

  @Parameters(value = "url")
  public SQLFindReferencesTest(@Optional String url) {
    super(url);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void findSimpleReference() {
    Collection<ODocument> result =
        database.command(new OCommandSQL("find references " + carID)).execute();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy"))
            .iterator()
            .next(),
        johnDoeID);

    // SUB QUERY
    result =
        database.command(new OCommandSQL("find references ( select from " + carID + ")")).execute();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy"))
            .iterator()
            .next(),
        johnDoeID);

    result = database.command(new OCommandSQL("find references " + chuckNorrisID)).execute();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy")).size(), 2);

    for (OIdentifiable rid :
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy"))) {
      Assert.assertTrue(rid.equals(ctuID) || rid.equals(fbiID));
    }

    result = database.command(new OCommandSQL("find references " + johnDoeID)).execute();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy")).size(), 0);

    result.clear();
    result = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void findReferenceByClassAndClusters() {
    Collection<ODocument> result =
        database
            .command(new OCommandSQL("find references " + janeDoeID + " [" + WORKPLACE + "]"))
            .execute();

    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy"))
            .iterator()
            .next()
            .equals(ctuID));

    result =
        database
            .command(
                new OCommandSQL(
                    "find references " + jackBauerID + " [" + WORKPLACE + ",cluster:" + CAR + "]"))
            .execute();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy")).size(), 3);

    for (OIdentifiable rid :
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy"))) {
      Assert.assertTrue(rid.equals(ctuID) || rid.equals(fbiID) || rid.equals(carID));
    }

    result =
        database
            .command(
                new OCommandSQL(
                    "find references "
                        + johnDoeID
                        + " ["
                        + WORKPLACE
                        + ","
                        + CAR
                        + ",cluster:"
                        + WORKER
                        + "]"))
            .execute();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(
        ((Collection<OIdentifiable>) result.iterator().next().field("referredBy")).size(), 0);

    result.clear();
    result = null;
  }

  @BeforeClass
  public void createTestEnvironment() {
    createSchema();
    populateDatabase();
  }

  private void createSchema() {
    OClass worker = database.getMetadata().getSchema().createClass(WORKER);
    OClass workplace = database.getMetadata().getSchema().createClass(WORKPLACE);
    OClass car = database.getMetadata().getSchema().createClass(CAR);

    worker.createProperty("name", OType.STRING);
    worker.createProperty("surname", OType.STRING);
    worker.createProperty("colleagues", OType.LINKLIST, worker);
    worker.createProperty("car", OType.LINK, car);

    workplace.createProperty("name", OType.STRING);
    workplace.createProperty("boss", OType.LINK, worker);
    workplace.createProperty("workers", OType.LINKLIST, worker);

    car.createProperty("plate", OType.STRING);
    car.createProperty("owner", OType.LINK, worker);
  }

  private void populateDatabase() {
    ODocument car = new ODocument(CAR);
    car.field("plate", "JINF223S");

    ODocument johnDoe = new ODocument(WORKER);
    johnDoe.field("name", "John");
    johnDoe.field("surname", "Doe");
    johnDoe.field("car", car);
    johnDoe.save();
    johnDoeID = johnDoe.getIdentity().copy();

    ODocument janeDoe = new ODocument(WORKER);
    janeDoe.field("name", "Jane");
    janeDoe.field("surname", "Doe");
    janeDoe.save();
    janeDoeID = janeDoe.getIdentity().copy();

    ODocument chuckNorris = new ODocument(WORKER);
    chuckNorris.field("name", "Chuck");
    chuckNorris.field("surname", "Norris");
    chuckNorris.save();
    chuckNorrisID = chuckNorris.getIdentity().copy();

    ODocument jackBauer = new ODocument(WORKER);
    jackBauer.field("name", "Jack");
    jackBauer.field("surname", "Bauer");
    jackBauer.save();
    jackBauerID = jackBauer.getIdentity().copy();

    ODocument ctu = new ODocument(WORKPLACE);
    ctu.field("name", "CTU");
    ctu.field("boss", jackBauer);
    List<ODocument> workplace1Workers = new ArrayList<ODocument>();
    workplace1Workers.add(chuckNorris);
    workplace1Workers.add(janeDoe);
    ctu.field("workers", workplace1Workers);
    ctu.save();
    ctuID = ctu.getIdentity().copy();

    ODocument fbi = new ODocument(WORKPLACE);
    fbi.field("name", "FBI");
    fbi.field("boss", chuckNorris);
    List<ODocument> workplace2Workers = new ArrayList<ODocument>();
    workplace2Workers.add(chuckNorris);
    workplace2Workers.add(jackBauer);
    fbi.field("workers", workplace2Workers);
    fbi.save();
    fbiID = fbi.getIdentity().copy();

    car.field("owner", jackBauer);
    car.save();
    carID = car.getIdentity().copy();
  }

  @AfterClass
  public void deleteTestEnvironment() {
    database.open("admin", "admin");

    carID.reset();
    carID = null;
    johnDoeID.reset();
    johnDoeID = null;
    janeDoeID.reset();
    janeDoeID = null;
    chuckNorrisID.reset();
    chuckNorrisID = null;
    jackBauerID.reset();
    jackBauerID = null;
    ctuID.reset();
    ctuID = null;
    fbiID.reset();
    fbiID = null;
    deleteSchema();

    database.close();
  }

  private void deleteSchema() {
    dropClass(CAR);
    dropClass(WORKER);
    dropClass(WORKPLACE);
  }

  private void dropClass(String iClass) {
    OCommandSQL dropClassCommand = new OCommandSQL("drop class " + iClass);
    database.command(dropClassCommand).execute();
    database.getMetadata().getSchema().reload();
    database.reload();
    while (database.getMetadata().getSchema().existsClass(iClass)) {
      database.getMetadata().getSchema().dropClass(iClass);
      database.reload();
    }
    while (database.getClusterIdByName(iClass) > -1) {
      database.dropCluster(iClass);
      database.reload();
    }
  }
}
