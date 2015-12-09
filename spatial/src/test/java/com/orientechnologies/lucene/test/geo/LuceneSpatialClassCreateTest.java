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

package com.orientechnologies.lucene.test.geo;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.lucene.test.BaseLuceneTest;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;

/**
 * Created by Enrico Risa on 06/08/15.
 */

@Test(groups = "embedded")
public class LuceneSpatialClassCreateTest extends BaseLuceneTest {

  @BeforeClass
  public void init() {
    initDB();
  }

  @Test
  public void testClasses() {

    OSchemaProxy schema = databaseDocumentTx.getMetadata().getSchema();

    Assert.assertNotNull(schema.getClass("OPoint"));

    Assert.assertNotNull(schema.getClass("OMultiPoint"));

    Assert.assertNotNull(schema.getClass("OLineString"));

    Assert.assertNotNull(schema.getClass("OMultiLineString"));

    Assert.assertNotNull(schema.getClass("ORectangle"));

    Assert.assertNotNull(schema.getClass("OPolygon"));

    Assert.assertNotNull(schema.getClass("OMultiPolygon"));
  }

  @AfterClass
  public void deInit() {
    deInitDB();
  }
}
