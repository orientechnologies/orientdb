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

package com.orientechnologies.lucene.test.local;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
//import com.orientechnologies.orient.spatial.shape.OShapeFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

//import com.orientechnologies.orient.spatial.shape.OPointShapeBuilder;

/**
 * Created by Enrico Risa on 31/08/15.
 */

public class ImportDataFromJson {

  @Test
  public void importAllFiles() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:databases/us_pacific");
    if (!db.exists()) {
      db.create();
    } else {
      db.open("admin", "admin");
    }

    try {
      importGeometry(db, "/Volumes/MACCHI/spatialIndex/us-pacific/us-pacific-latest_points.json", "Points", "OPoint");
      importGeometry(db, "/Volumes/MACCHI/spatialIndex/us-pacific/us-pacific-latest_lines.json", "Lines", "OLineString");
      importGeometry(db, "/Volumes/MACCHI/spatialIndex/us-pacific/us-pacific-latest_multipolygons.json", "MultiPolygons",
          "OMultipolygon");
      importGeometry(db, "/Volumes/MACCHI/spatialIndex/us-pacific/us-pacific-latest_multilinestrings.json", "MultiLineStrings",
              "OMultiLineString");

      importGeometry(db, "/Volumes/MACCHI/spatialIndex/us-pacific/us-pacific-latest_other_relations.json", "GeometryCollections",
              "OGeometryCollection");
    } catch (IOException e1) {
      e1.printStackTrace();
    }

  }

  protected void importGeometry(ODatabaseDocumentTx db, String file, final String clazz, String geomClazz) throws IOException {

    OClass points = db.getMetadata().getSchema().createClass(clazz);
    points.createProperty("geometry", OType.EMBEDDED, db.getMetadata().getSchema().getClass(geomClazz));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    OIOUtils.copyStream(new FileInputStream(new File(file)), out, -1);
    ODocument doc = new ODocument().fromJSON(out.toString(), "noMap");
    List<ODocument> collection = doc.field("collection");

//    OShapeFactory builder = OShapeFactory.INSTANCE;

    final AtomicLong atomicLong = new AtomicLong(0);

    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        OLogManager.instance().info(this, clazz + " per second [%d]", atomicLong.get());
        atomicLong.set(0);
      }
    };
    Orient.instance().scheduleTask(task, 1000, 1000);
    for (ODocument entries : collection) {
      ODocumentInternal.removeOwner(entries, doc);
      ODocumentInternal.removeOwner(entries, (ORecordElement) collection);
      entries.setClassName(clazz);
      String wkt = entries.field("GeometryWkt");
      try {
//        ODocument location = builder.toDoc(wkt);
//        entries.field("geometry", location, OType.EMBEDDED);
//        db.save(entries);
//
//        atomicLong.incrementAndGet();
      } catch (Exception e) {

      }
    }
    task.cancel();
  }
}
