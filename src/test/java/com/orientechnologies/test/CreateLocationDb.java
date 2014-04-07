package com.orientechnologies.test;

import au.com.bytecode.opencsv.CSVReader;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.io.FileReader;
import java.io.IOException;

/**
 * Created by enricorisa on 07/04/14.
 */
public class CreateLocationDb {

  @Test
  public void createDb() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/location");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();

    }
    databaseDocumentTx.create();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");
    oClass.createProperty("latitude", OType.DOUBLE);
    oClass.createProperty("longitude", OType.DOUBLE);
    oClass.createIndex("City.latitude_longitude", "SPATIAL", null, null, "LUCENE", new String[] { "latitude", "longitude" });
    CSVReader reader = new CSVReader(new FileReader("files/location.csv"), ',');
    String[] nextLine;

    databaseDocumentTx.begin();
    int i = 0;
    while ((nextLine = reader.readNext()) != null) {

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
    databaseDocumentTx.commit();
    databaseDocumentTx.close();

  }
}
