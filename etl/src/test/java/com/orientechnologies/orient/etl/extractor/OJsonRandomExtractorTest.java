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

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import org.junit.Test;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLBaseTest;
import com.orientechnologies.orient.etl.OETLStubRandomExtractor;

/**
 * Tests ETL JSON Extractor.
 *
 * @author Luca Garulli
 */
public class OJsonRandomExtractorTest extends OETLBaseTest {

  private final static int TOTAL = 1000000;

  @Ignore
  public void testNonParallel() {
    proc.getFactory().registerExtractor(OETLStubRandomExtractor.class);

    process("{extractor : { random: {items: " + TOTAL + ", fields: 10} }, "
        + "loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', class: 'Person', useLightweightEdges:false, "
        + "classes: [{name: 'Person', extends: 'V'}] } } }");

    assertEquals(TOTAL, graph.countVertices("Person"));

    int i = 0;
    for (ODocument doc : graph.getRawGraph().browseClass("Person")) {
      assertEquals(10, doc.fields());
      i++;
    }
  }

  @Test
  public void testParallel() {
    proc.getFactory().registerExtractor(OETLStubRandomExtractor.class);

    process(
        "{extractor : { random: {items: " + TOTAL + ", fields: 10, delay: 0} }, "
            + "loader: { orientdb: { dbURL: 'plocal:./target/OETLBaseTest', dbType:'graph', class: 'Person', useLightweightEdges:false, "
            + "classes: [{name: 'Person', extends: 'V', clusters: 8 }] } } }",
        new OBasicCommandContext().setVariable("parallel", Boolean.TRUE).setVariable("dumpEveryMs", 1000));

    assertEquals(TOTAL, graph.countVertices("Person"));

    int i = 0;
    for (ODocument doc : graph.getRawGraph().browseClass("Person")) {
      assertEquals(10, doc.fields());
      i++;
    }
  }
}
