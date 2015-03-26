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
import com.orientechnologies.orient.etl.OETLProcessor;
import org.junit.Test;

/**
 * Tests ETL CSV Transformer.
 *
 * @author Luca Garulli
 */
public class CSVTransformerTest extends ETLBaseTest {

  @Test
  public void testEmpty() {
    OETLProcessor proc = getProcessor("{source: { content: { value: '' }  }, extractor : { json: {} }, loader: { test: {} } }").execute();
    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 0);
  }

  @Test
  public void testOneObject() {
    OETLProcessor proc = getProcessor("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }], loader: { test: {} } }").execute();
    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 1);
    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(doc.fields(), 2);
    assertEquals(doc.field("name"), "Jay");
    assertEquals(doc.field("surname"), "Miner");
  }

  @Test
  public void testSmallSet() {
    String content = "name,surname,id";
    for (int i = 0; i < names.length; ++i)
      content += "\n" + names[i] + "," + surnames[i] + "," + i;
    OETLProcessor proc = getProcessor("{source: { content: { value: '" + content + "' } }, extractor : { row: {} }, transformers: [{ csv: {} }], loader: { test: {} } }").execute();

    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), names.length);

    int i = 0;
    for (ODocument doc : ((TestLoader) proc.getLoader()).getResult()) {
      assertEquals(doc.fields(), 3);
      assertEquals(doc.field("name"), names[i]);
      assertEquals(doc.field("surname"), surnames[i]);
      assertEquals(doc.field("id"), i);
      i++;
    }
  }
}
