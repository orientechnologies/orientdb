/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by enricorisa on 02/10/14. */
public class LuceneSpatialQueryTest extends BaseLuceneTest {

  @Before
  public void init() {
    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");

    OClass oClass = schema.createClass("Place");
    oClass.setSuperClass(v);
    oClass.createProperty("latitude", OType.DOUBLE);
    oClass.createProperty("longitude", OType.DOUBLE);
    oClass.createProperty("name", OType.STRING);

    db.command(
            new OCommandSQL(
                "CREATE INDEX Place.l_lon ON Place(latitude,longitude) SPATIAL ENGINE LUCENE"))
        .execute();

    try {
      ZipFile zipFile =
          new ZipFile(new File(ClassLoader.getSystemResource("location.csv.zip").getPath()));
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      db.declareIntent(new OIntentMassiveInsert());
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();

        Orient.instance()
            .scheduleTask(
                new Runnable() {
                  @Override
                  public void run() {

                    Runtime runtime = Runtime.getRuntime();

                    StringBuilder sb = new StringBuilder();
                    long maxMemory = runtime.maxMemory();
                    long allocatedMemory = runtime.totalMemory();
                    long freeMemory = runtime.freeMemory();
                    OLogManager.instance()
                        .info(
                            this,
                            "Memory Stats: free [%d], allocated [%d], max [%d] total free [%d]",
                            freeMemory / 1024,
                            allocatedMemory / 1024,
                            maxMemory / 1024,
                            (freeMemory + (maxMemory - allocatedMemory)) / 1024);
                  }
                },
                10000,
                10000);
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
              db.commit();
              db.begin();
            }
            i++;
          }
          lnr.close();
          stream.close();
          db.commit();
        }
        db.declareIntent(null);
      }

    } catch (Exception e) {

    } finally {

    }
  }

  @Test
  @Ignore
  public void testNearQuery() {

    String query =
        "select *,$distance from Place where [latitude,longitude,$spatial] NEAR [41.893056,12.482778,{\"maxDistance\": 0.5}]";
    List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(1, docs.size());

    // WHY ? 0.2749329729746763
    // Assert.assertEquals(0.27504313167833594, docs.get(0).field("$distance"));
    //    Assert.assertEquals(, docs.get(0).field("$distance"));

    assertThat(docs.get(0).<Float>field("$distance")).isEqualTo(0.2749329729746763);
  }

  @Test
  @Ignore
  public void testWithinQuery() {
    String query =
        "select * from Place where [latitude,longitude] WITHIN [[51.507222,-0.1275],[55.507222,-0.1275]]";
    List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(238, docs.size());
  }
}
