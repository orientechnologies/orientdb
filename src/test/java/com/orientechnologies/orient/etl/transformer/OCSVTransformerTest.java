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

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.ETLBaseTest;
import org.junit.Test;

import java.util.Date;
import java.util.List;

/**
 * Tests ETL CSV Transformer.
 *
 * @author Luca Garulli
 */
public class OCSVTransformerTest extends ETLBaseTest {

  @Test
  public void testEmpty() {
    String cfgJson = "{source: { content: { value: '' }  }, extractor : { json: {} }, loader: { test: {} } }";
    process(cfgJson);
    assertEquals(0, getResult().size());
  }

  @Test
  public void testOneObject() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { row: {} }, transformers: [{ csv: {} }], loader: { test: {} } }");
    assertEquals(1, getResult().size());
    ODocument doc = getResult().get(0);
    assertEquals(2, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
  }

  @Test
  public void testSmallSet() {
    String content = "name,surname,id";
    for (int i = 0; i < names.length; ++i)
      content += "\n" + names[i] + "," + surnames[i] + "," + i;
    process("{source: { content: { value: '" + content + "' } }, extractor : { row: {} }, transformers: [{ csv: {} }], loader: { test: {} } }");

    assertEquals(getResult().size(), names.length);

    int i = 0;
    for (ODocument doc : getResult()) {
      assertEquals(3, doc.fields());
      assertEquals(names[i], doc.field("name"));
      assertEquals(surnames[i], doc.field("surname"));
      assertEquals(i, doc.field("id"));
      i++;
    }
  }
    @Test
    public void testDateTypeAutodetection(){
        String cfgJson = "{source: { content: { value: 'BirthDay\n2008-04-30' }  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
        process(cfgJson);
        List<ODocument> res = getResult();
        ODocument doc = res.get(0);
        Date birthday = doc.field("BirthDay");
        assertEquals(2008, birthday.getYear());
        assertEquals(3, birthday.getMonth());
        assertEquals(30, birthday.getDate());
    }

    @Test
    public void testStringInDblQoutes() throws Exception {
        String cfgJson = "{source: { content: { value: 'text\n\"Hello, quotes are here!\"' }  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
        process(cfgJson);
        List<ODocument> res = getResult();
        ODocument doc = res.get(0);
        String text = doc.field("text");
        assertEquals("Hello, quotes are here!", text);
    }
}
