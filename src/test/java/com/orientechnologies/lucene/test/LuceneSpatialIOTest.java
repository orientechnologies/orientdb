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

import com.orientechnologies.lucene.shape.OPointShapeBuilder;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Point;
import junit.framework.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;

/**
 * Created by Enrico Risa on 06/08/15.
 */
public class LuceneSpatialIOTest extends BaseLuceneTest {
  @Override
  protected String getDatabaseName() {
    return "conversionTest";
  }

  @BeforeClass
  public void init() {
    initDB();
  }

  @Test
  public void testPointIO() {

    ODocument doc = new ODocument("Point");
    doc.field("coordinates", new ArrayList<Double>() {
      {
        add(-100d);
        add(45d);
      }
    });
    doc.save();
    OPointShapeBuilder builder = new OPointShapeBuilder();

    String p1 = builder.asText(doc);
    Assert.assertNotNull(p1);

    Point point = SpatialContext.GEO.makePoint(-100d, 45d);

    String p2 = JtsSpatialContext.GEO.getGeometryFrom(point).toText();

    Assert.assertEquals(p2, p1);
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
