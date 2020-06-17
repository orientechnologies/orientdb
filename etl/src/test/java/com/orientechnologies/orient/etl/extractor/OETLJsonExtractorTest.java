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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

/**
 * Tests ETL JSON Extractor.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OETLJsonExtractorTest extends OETLBaseTest {

  @Test
  public void testEmptyCollection() {
    configure(
        "{source: { content: { value: [] }  }, extractor : { json: {} }, loader: { test: {} } }");
    proc.execute();

    assertEquals(0, getResult().size());
  }

  @Test
  public void testEmptyObject() {
    configure(
        "{source: { content: { value: {} }  }, extractor : { json: {} }, loader: { test: {} } }");
    proc.execute();

    assertEquals(1, getResult().size());
    ODocument doc = getResult().get(0);
    assertEquals(0, doc.fields());
  }

  @Test
  public void testOneObject() {
    configure(
        "{source: { content: { value: { name: 'Jay', surname: 'Miner' } } }, extractor : { json: {} }, loader: { test: {} } }");
    proc.execute();

    assertEquals(1, getResult().size());
    ODocument doc = getResult().get(0);
    assertEquals(2, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
  }

  @Test
  public void testSmallSet() {
    String content = "";
    for (int i = 0; i < names.length; ++i) {
      if (i > 0) content += ",";
      content += "{name:'" + names[i] + "',surname:'" + surnames[i] + "',id:" + i + "}";
    }

    configure(
        "{source: { content: { value: ["
            + content
            + "] } }, extractor : { json: {} }, loader: { test: {} } }");
    proc.execute();

    assertEquals(names.length, getResult().size());

    int i = 0;
    for (ODocument doc : getResult()) {
      assertEquals(3, doc.fields());
      assertEquals(names[i], doc.field("name"));
      assertEquals(surnames[i], doc.field("surname"));

      assertThat(doc.<Integer>field("id")).isEqualTo(i);
      i++;
    }
  }

  @Test
  public void testHaltOnBadInput() {

    configure(
        "{\"source\": {\n"
            + "    \"file\": {\n"
            + "      \"path\": \"./src/test/resources/comments.json\"\n"
            + "    }\n"
            + "  }, extractor : { json: {} }, loader: { test: {} } }");
    proc.execute();

    assertThat(getResult().size()).isEqualTo(2);
    int i = 0;
    for (ODocument doc : getResult()) {
      assertThat(doc.<Integer>field("id")).isLessThan(5);
    }
  }

  @Test
  public void testSkipOnBadInput() {

    configure(
        " { \"config\": {\n"
            + "    \"haltOnError\": false\n"
            + "  },"
            + "\"source\": {\n"
            + "    \"file\": {\n"
            + "      \"path\": \"./src/test/resources/comments.json\"\n"
            + "    }\n"
            + "  }, extractor : { json: {} }, loader: { test: {} } }");
    proc.execute();

    assertThat(getResult().size()).isEqualTo(4);
    int i = 0;
    for (ODocument doc : getResult()) {
      //      assertThat(doc.<Integer>field("id")).isLessThan(5);
    }
  }
}
