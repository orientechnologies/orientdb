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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import junit.framework.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Enumeration;
import java.util.List;
import java.util.TimerTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by enricorisa on 02/10/14.
 */
@Test(groups = "embedded")
public class LuceneSpatialQueryTest extends BaseLuceneTest {

  public LuceneSpatialQueryTest() {
  }

  public LuceneSpatialQueryTest(boolean remote) {
    super();
  }

  @Override
  protected String getDatabaseName() {
    return "luceneSpatial";
  }

  @BeforeClass
  public void init() {
    initDB();
    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    OClass v = schema.getClass("V");

    OClass oClass = schema.createClass("Place");
    oClass.setSuperClass(v);
    oClass.createProperty("latitude", OType.DOUBLE);
    oClass.createProperty("longitude", OType.DOUBLE);
    oClass.createProperty("name", OType.STRING);

    databaseDocumentTx.command(new OCommandSQL("CREATE INDEX Place.l_lon ON Place(latitude,longitude) SPATIAL ENGINE LUCENE"))
        .execute();

    try {
      ZipFile zipFile = new ZipFile(new File(ClassLoader.getSystemResource("location.csv.zip").getPath()));
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      databaseDocumentTx.declareIntent(new OIntentMassiveInsert());
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();

        Orient.instance().getTimer().schedule(new TimerTask() {
          @Override
          public void run() {

            Runtime runtime = Runtime.getRuntime();

            StringBuilder sb = new StringBuilder();
            long maxMemory = runtime.maxMemory();
            long allocatedMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            OLogManager.instance().info(this, "Memory Stats: free [%d], allocated [%d], max [%d] total free [%d]",
                freeMemory / 1024, allocatedMemory / 1024, maxMemory / 1024, (freeMemory + (maxMemory - allocatedMemory)) / 1024);
          }
        }, 10000, 10000);
        if (entry.getName().equals("location.csv")) {

          InputStream stream = zipFile.getInputStream(entry);
          LineNumberReader lnr = new LineNumberReader(new InputStreamReader(stream));

          String line;
          int i = 0;
          while ((line = lnr.readLine()) != null) {
            String[] nextLine = line.split(",");
            ODocument doc = new ODocument("Place");
            doc.field("name", nextLine[3]);
            doc.field("country", nextLine[1]);
            try {

              Double lat = ((Double) OType.convert(nextLine[5], Double.class)).doubleValue();
              Double lng = ((Double) OType.convert(nextLine[6], Double.class)).doubleValue();
              doc.field("latitude", lat);
              doc.field("longitude", lng);
            } catch (Exception e) {
              continue;
            }

            doc.save();
            if (i % 100000 == 0) {
              OLogManager.instance().info(this, "Imported: [%d] records", i);
              databaseDocumentTx.commit();
              databaseDocumentTx.begin();
            }
            i++;
          }
          lnr.close();
          stream.close();
          databaseDocumentTx.commit();
        }
        databaseDocumentTx.declareIntent(null);
      }

    } catch (Exception e) {

    } finally {

    }

  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }

  @Test(priority = 1)
  public void testNearQuery() {

    assertNear();
  }

  @Test(priority = 3)
  public void testNearQueryWithoutIndex() {

    databaseDocumentTx.command(new OCommandSQL("drop index Place.l_lon")).execute();
    assertNear();
  }

  @Test(priority = 4)
  public void testWithinQueryWithoutIndex() {
    databaseDocumentTx.command(new OCommandSQL("drop index Place.l_lon")).execute();
    assertWithin();
  }

  protected void assertNear() {
    String query = "select *,$distance from Place where [latitude,longitude,$spatial] NEAR [41.893056,12.482778,{\"maxDistance\": 0.5}]";
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    Assert.assertEquals(0.2749329729746763, docs.get(0).field("$distance"));
  }

  @Test(priority = 2)
  public void testWithinQuery() {
    assertWithin();
  }

  protected void assertWithin() {
    String query = "select * from Place where [latitude,longitude] WITHIN [[51.286839,-0.51035],[51.692322,0.33403]]";
    List<ODocument> docs = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(291, docs.size());
  }
}
