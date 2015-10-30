package com.orientechnologies.lucene.test;

import au.com.bytecode.opencsv.CSVReader;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by enricorisa on 07/04/14.
 */
public class CreateLocationDb extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private static CSVReader    reader;
  private static int          cycleNumber;
  private int                 i = 0;

  static {

    try {

      ZipFile zipFile = new ZipFile("files/location.csv.zip");
      Enumeration<? extends ZipEntry> entries = zipFile.entries();

      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (entry.getName().equals("location.csv")) {
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

  @Parameters(value = "url")
  public CreateLocationDb(String url) {
    super(cycleNumber);
    System.out.println(url);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/location");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");
    oClass.createProperty("latitude", OType.DOUBLE);
    oClass.createProperty("longitude", OType.DOUBLE);
    oClass.createProperty("name", OType.STRING);
    oClass.createIndex("City.latitude_longitude", "SPATIAL", null, null, "LUCENE", new String[] { "latitude", "longitude" });
    oClass.createIndex("City.name", "FULLTEXT", null, null, "LUCENE", new String[] { "name" });
    ZipFile zipFile = new ZipFile("files/location.csv.zip");
    Enumeration<? extends ZipEntry> entries = zipFile.entries();

    while (entries.hasMoreElements()) {

      ZipEntry entry = entries.nextElement();
      if (entry.getName().equals("location.csv")) {

        InputStream stream = zipFile.getInputStream(entry);
        reader = new CSVReader(new InputStreamReader(stream), ',');
      }
    }

  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {

    String[] nextLine = reader.readNext();
    if (nextLine != null) {
      ODocument doc = new ODocument("City");
      doc.field("name", nextLine[3]);
      doc.field("country", nextLine[1]);
      doc.field("region", nextLine[2]);
      doc.field("zip", nextLine[4]);
      Double lat = ((Double) OType.convert(nextLine[5], Double.class)).doubleValue();
      Double lng = ((Double) OType.convert(nextLine[6], Double.class)).doubleValue();
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
