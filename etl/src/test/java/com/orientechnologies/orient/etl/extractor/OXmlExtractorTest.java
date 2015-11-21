/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.ETLBaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests ETL XML Extractor.
 *
 * @author Luca Garulli
 */
public class OXmlExtractorTest extends ETLBaseTest {

  @Test
  public void testSimpleXml() {
    process("{source: { file: { path: 'src/test/resources/simple.xml' } }, extractor : { xml: {} }, loader: { test: {} } }");
    assertEquals(1, getResult().size());
    ODocument doc = getResult().get(0);

    System.out.println(doc.toJSON("prettyPrint"));
  }

  @Test
  public void testCollectionXml() {
    process("{source: { file: { path: 'src/test/resources/music.xml' } }, extractor : { xml: {} }, loader: { test: {} } }");
    assertEquals(1, getResult().size());
    ODocument doc = getResult().get(0);

    System.out.println(doc.toJSON("prettyPrint"));
  }
}
