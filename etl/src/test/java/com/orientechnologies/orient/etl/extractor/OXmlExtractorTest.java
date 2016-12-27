/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests ETL XML Extractor.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OXmlExtractorTest extends OETLBaseTest {

  @Test
  public void testSimpleXml() {
    process("{source: { file: { path: 'src/test/resources/simple.xml' } }, extractor : { xml: {} }, loader: { test: {} } }");
    assertEquals(1, getResult().size());
    ODocument doc = getResult().get(0);

    assertNotNull(doc.field("a"));
    ODocument a = (ODocument) doc.field("a");

    assertNotNull(a.field("b"));
    ODocument b = (ODocument) a.field("b");

    assertNotNull(b.field("c"));
    Collection<ODocument> c = (Collection<ODocument>) b.field("c");

    assertEquals(2, c.size());
    final Iterator<ODocument> it = c.iterator();

    final ODocument ferrari = it.next();
    assertNotNull(ferrari);
    assertEquals("Ferrari", ferrari.field("name"));
    assertEquals("red", ferrari.field("color"));

    final ODocument maserati = it.next();
    assertNotNull(maserati);
    assertEquals("Maserati", maserati.field("name"));
    assertEquals("black", maserati.field("color"));

//    System.out.println(doc.toJSON("prettyPrint"));
  }

  @Test
  public void testCollectionXml() {
    process(
        "{source: { file: { path: 'src/test/resources/music.xml' } }, extractor : { xml: { rootNode: 'CATALOG.CD', tagsAsAttribute: ['CATALOG.CD'] } }, loader: { test: {} } }");
    assertEquals(3, getResult().size());

    final List<ODocument> cds = getResult();
    final Iterator<ODocument> it = cds.iterator();

//    for (int i = 0; i<getResult().size(); ++i)
//      System.out.println(cds.get(i).toJSON("prettyPrint"));

    final ODocument doc1 = it.next();
    assertNotNull(doc1);
    assertEquals("Empire Burlesque", doc1.field("TITLE"));

    final ODocument doc2 = it.next();
    assertNotNull(doc2);
    assertEquals("Hide your heart", doc2.field("TITLE"));

    final ODocument doc3 = it.next();
    assertNotNull(doc3);
    assertEquals("Greatest Hits", doc3.field("TITLE"));
  }
}
