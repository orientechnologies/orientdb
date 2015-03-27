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
import com.orientechnologies.orient.etl.OETLProcessor;
import com.orientechnologies.orient.etl.TestLoader;
import org.junit.Test;

/**
 * Tests ETL JSON Extractor.
 *
 * @author Luca Garulli
 */
public class JsonExtractorTest extends ETLBaseTest {

  @Test
  public void testEmptyCollection() {
    getProcessor("{source: { content: { value: [] }  }, extractor : { json: {} }, loader: { test: {} } }").execute();
    assertEquals(0, ((TestLoader) proc.getLoader()).getResult().size());
  }

  @Test
  public void testEmptyObject() {
    getProcessor("{source: { content: { value: {} }  }, extractor : { json: {} }, loader: { test: {} } }").execute();
    assertEquals(1, ((TestLoader) proc.getLoader()).getResult().size());
    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(0, doc.fields());
  }

  @Test
  public void testOneObject() {
    getProcessor("{source: { content: { value: { name: 'Jay', surname: 'Miner' } } }, extractor : { json: {} }, loader: { test: {} } }").execute();
    assertEquals(1, ((TestLoader) proc.getLoader()).getResult().size());
    ODocument doc = ((TestLoader) proc.getLoader()).getResult().get(0);
    assertEquals(2, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
  }

  @Test
  public void testSmallSet() {
    String content = "";
    for (int i = 0; i < names.length; ++i) {
      if (i > 0)
        content += ",";
      content += "{name:'" + names[i] + "',surname:'" + surnames[i] + "',id:" + i + "}";
    }

    getProcessor("{source: { content: { value: [" + content + "] } }, extractor : { json: {} }, loader: { test: {} } }").execute();

    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), names.length);

    int i = 0;
    for (ODocument doc : ((TestLoader) proc.getLoader()).getResult()) {
      assertEquals(3, doc.fields());
      assertEquals(names[i], doc.field("name"));
      assertEquals(surnames[i], doc.field("surname"));
      assertEquals(i, doc.field("id"));
      i++;
    }
  }
}
