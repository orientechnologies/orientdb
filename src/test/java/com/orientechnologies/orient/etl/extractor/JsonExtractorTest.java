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

/**
 * Tests ETL JSON Extractor.
 *
 * @author Luca Garulli
 */
public class JsonExtractorTest extends ETLBaseTest {

  public void testEmptyCollection() {
    OETLProcessor proc = getProcessor("{source: { content: { value: [] }  }, extractor : { json: {} }, loader: { test: {} } }").execute();
    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 0);
  }

  public void testEmptyObject() {
    OETLProcessor proc = getProcessor("{source: { content: { value: {} }  }, extractor : { json: {} }, loader: { test: {} } }").execute();
    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 1);
    ODocument doc = ((ODocument) ((TestLoader) proc.getLoader()).getResult().get(0));
    assertEquals(doc.fields(), 0);
  }

  public void testOneObject() {
    OETLProcessor proc = getProcessor("{source: { content: { value: { name: 'Jay', surname: 'Miner' } } }, extractor : { json: {} }, loader: { test: {} } }").execute(); assertEquals(((TestLoader) proc.getLoader()).getResult().size(), 1);
    ODocument doc = ((ODocument) ((TestLoader) proc.getLoader()).getResult().get(0));
    assertEquals(doc.fields(), 2);
    assertEquals(doc.field("name"), "Jay");
    assertEquals(doc.field("surname"), "Miner");
  }

  public void testSmallSet() {
    String content = "";
    for (int i = 0; i < names.length; ++i) {
      if (i > 0)
        content += ",";
      content += "{name:'" + names[i] + "',surname:'" + surnames[i] + "',id:" + i + "}";
    }

    OETLProcessor proc = getProcessor(
        "{source: { content: { value: [" + content + "] } }, extractor : { json: {} }, loader: { test: {} } }").execute();

    assertEquals(((TestLoader) proc.getLoader()).getResult().size(), names.length);

    int i = 0;
    for (Object o : ((TestLoader) proc.getLoader()).getResult()) {
      ODocument doc = (ODocument) o;
      assertEquals(doc.fields(), 3);
      assertEquals(doc.field("name"), names[i]);
      assertEquals(doc.field("surname"), surnames[i]);
      assertEquals(doc.field("id"), i);
      i++;
    }
  }
}
