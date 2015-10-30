/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.test;

import au.com.bytecode.opencsv.CSVReader;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by enricorisa on 07/04/14.
 */
public class CreateCityDb extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private static CSVReader    reader;
  private static int          cycleNumber;
  private int                 i = 0;
  private LineNumberReader    lineReader;

  static {

    try {

      ZipFile zipFile = new ZipFile("files/allCountries.zip");
      Enumeration<? extends ZipEntry> entries = zipFile.entries();

      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (entry.getName().equals("allCountries.txt")) {
          InputStream stream = zipFile.getInputStream(entry);
          LineNumberReader lnr = new LineNumberReader(new InputStreamReader(stream));
          int linenumber = 0;
          while (lnr.readLine() != null) {
            linenumber++;
          }
          lnr.close();
          cycleNumber = linenumber;
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public CreateCityDb() {
    super(cycleNumber);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    String uri = "plocal:" + buildDirectory + "/databases/city";
    databaseDocumentTx = new ODatabaseDocumentTx(uri);
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }
    databaseDocumentTx.create();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    if (schema.getClass("City") == null) {
      OClass oClass = schema.createClass("City");
      oClass.createProperty("latitude", OType.DOUBLE);
      oClass.createProperty("longitude", OType.DOUBLE);
      oClass.createProperty("name", OType.STRING);
      oClass.createIndex("City.name", "FULLTEXT", null, null, "LUCENE", new String[] { "name" });
      oClass.createIndex("City.lat_lng", "SPATIAL", null, null, "LUCENE", new String[] { "latitude", "longitude" });
    }
    ZipFile zipFile = new ZipFile("files/allCountries.zip");
    Enumeration<? extends ZipEntry> entries = zipFile.entries();

    while (entries.hasMoreElements()) {

      ZipEntry entry = entries.nextElement();
      if (entry.getName().equals("allCountries.txt")) {

        InputStream stream = zipFile.getInputStream(entry);
        lineReader = new LineNumberReader(new InputStreamReader(stream));
      }
    }

    databaseDocumentTx.declareIntent(new OIntentMassiveInsert());
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {

    String readed = lineReader.readLine();
    String[] nextLine = readed.split("\\t");
    if (readed != null) {
      ODocument doc = new ODocument("City");
      doc.field("name", nextLine[1]);
      doc.field("country", nextLine[8]);
      Double lat = ((Double) OType.convert(nextLine[4], Double.class)).doubleValue();
      Double lng = ((Double) OType.convert(nextLine[5], Double.class)).doubleValue();
      doc.field("latitude", lat);
      doc.field("longitude", lng);
      doc.save();
      if (i % 1000 == 0) {
        databaseDocumentTx.commit();
        databaseDocumentTx.begin();
      }
      i++;
    }

  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    databaseDocumentTx.commit();
    databaseDocumentTx.close();
  }
}
