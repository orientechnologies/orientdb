/*
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

package com.orientechnologies.orient.object.enhancement.field;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;

/**
 * @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a>
 */
public class OObjectBinaryDataStorageTest {

  private OObjectDatabaseTx databaseTx;

  @After
  public void tearDown() {
    databaseTx.drop();
  }

  @Test
  public void testSaveAndLoad_BinaryFieldsSimpleRecordMapping() throws IOException {

    // setup
    this.createDb(ODocumentFieldHandlingStrategyFactory.SIMPLE);

    Driver hunt = new Driver();
    hunt.setName("James Hunt");
    byte[] huntUglyPicture = randomBytes(1024 * 32);
    hunt.setImageData(huntUglyPicture);

    // exercise
    Driver savedHunt = this.databaseTx.save(hunt);
    Driver loadedHunt = this.databaseTx.load(new ORecordId(savedHunt.getId()));

    // verify
    Assert.assertNotNull(savedHunt);
    Assert.assertNotNull(loadedHunt);
    Assert.assertArrayEquals(huntUglyPicture, hunt.getImageData());
    Assert.assertArrayEquals(huntUglyPicture, savedHunt.getImageData());
    Assert.assertArrayEquals(huntUglyPicture, loadedHunt.getImageData());
  }

  @Test
  public void testSaveAndLoad_BinaryFieldsSingleRecordMapping() throws IOException {

    // setup
    this.createDb(ODocumentFieldHandlingStrategyFactory.SINGLE_ORECORD_BYTES);

    Driver lauda = new Driver();
    lauda.setName("Niki Lauda");
    byte[] laudaRealisticPicture = randomBytes(1024 * 64);
    lauda.setImageData(laudaRealisticPicture);

    // exercise
    Driver savedLauda = this.databaseTx.save(lauda);
    Driver loadedLauda = this.databaseTx.load(new ORecordId(savedLauda.getId()));

    // verify
    Assert.assertNotNull(savedLauda);
    Assert.assertNotNull(loadedLauda);
    Assert.assertArrayEquals(laudaRealisticPicture, lauda.getImageData());
    Assert.assertArrayEquals(laudaRealisticPicture, savedLauda.getImageData());
    Assert.assertArrayEquals(laudaRealisticPicture, loadedLauda.getImageData());
  }

  @Test
  public void testSaveAndLoad_BinaryFieldsSplitRecordMapping() throws IOException {

    // setup
    this.createDb(ODocumentFieldHandlingStrategyFactory.SPLIT_ORECORD_BYTES);

    Driver prost = new Driver();
    prost.setName("Alain Prost");
    byte[] prostUglyPicture = randomBytes(1024 * 128 + 1);
    prost.setImageData(prostUglyPicture);

    // exercise
    Driver savedProst = this.databaseTx.save(prost);
    Driver loadedProst = this.databaseTx.load(new ORecordId(savedProst.getId()));

    // verify
    Assert.assertNotNull(savedProst);
    Assert.assertNotNull(loadedProst);
    Assert.assertArrayEquals(prostUglyPicture, prost.getImageData());
    Assert.assertArrayEquals(prostUglyPicture, savedProst.getImageData());
    Assert.assertArrayEquals(prostUglyPicture, loadedProst.getImageData());
  }

  @Test
  public void testSaveAndLoad_DefaultRecordMapping() throws IOException {

    // setup
    this.createDb(-1);

    Driver monzasGorilla = new Driver();
    monzasGorilla.setName("Vittorio Brambilla");
    byte[] brambillaPicture = randomBytes(1024 * 32);
    monzasGorilla.setImageData(brambillaPicture);

    // exercise
    Driver savedBrambilla = this.databaseTx.save(monzasGorilla);
    Driver loadedBrambilla = this.databaseTx.load(new ORecordId(savedBrambilla.getId()));

    // verify
    Assert.assertNotNull(savedBrambilla);
    Assert.assertNotNull(loadedBrambilla);
    Assert.assertArrayEquals(brambillaPicture, monzasGorilla.getImageData());
    Assert.assertArrayEquals(brambillaPicture, savedBrambilla.getImageData());
    Assert.assertArrayEquals(brambillaPicture, loadedBrambilla.getImageData());
  }

  @Test
  public void testSaveAndLoad_BinaryFieldsSimpleRecordMapping_InstantiatePojoUsingDbFactory() throws IOException {

    // setup
    this.createDb(ODocumentFieldHandlingStrategyFactory.SIMPLE);

    Driver ronnie = this.databaseTx.newInstance(Driver.class);
    ronnie.setName("Ronnie Peterson");
    byte[] ronniePicture = randomBytes(1024 * 32);
    ronnie.setImageData(ronniePicture);

    // exercise
    Driver savedRonnie = this.databaseTx.save(ronnie);
    Driver loadedRonnie = this.databaseTx.load(new ORecordId(savedRonnie.getId()));

    // verify
    Assert.assertNotNull(savedRonnie);
    Assert.assertNotNull(loadedRonnie);
    Assert.assertArrayEquals(ronniePicture, ronnie.getImageData());
    Assert.assertArrayEquals(ronniePicture, savedRonnie.getImageData());
    Assert.assertArrayEquals(ronniePicture, loadedRonnie.getImageData());
  }

  private void createDb(int strategy) {

    // Store strategy setting
    if (strategy >= 0) {
      OGlobalConfiguration.DOCUMENT_BINARY_MAPPING.setValue(strategy);
    }

    // Create
    databaseTx = new OObjectDatabaseTx("memory:" + this.getClass().getSimpleName());
    databaseTx.create();

    databaseTx.getEntityManager().registerEntityClass(Driver.class);
  }

  private byte[] randomBytes(int size) {

    byte[] b = new byte[size];
    new Random().nextBytes(b);

    return b;
  }
}
