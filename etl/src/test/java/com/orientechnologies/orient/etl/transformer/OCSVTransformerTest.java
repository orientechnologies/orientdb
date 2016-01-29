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
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Tests ETL CSV Transformer.
 *
 * @author Luca Garulli
 */
public class OCSVTransformerTest extends OETLBaseTest {

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
    process("{source: { content: { value: '" + content
        + "' } }, extractor : { row: {} }, transformers: [{ csv: {} }], loader: { test: {} } }");

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
  public void testDateTypeAutodetection() {
    String cfgJson = "{source: { content: { value: 'BirthDay\n2008-04-30' }  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    Date birthday = doc.field("BirthDay");
    assertEquals(2008, birthday.getYear() + 1900);
    assertEquals(4, birthday.getMonth() + 1);
    assertEquals(30, birthday.getDate());
  }

  @Test
  public void testStringInDblQuotes() throws Exception {
    String cfgJson = "{source: { content: { value: 'text\n\"Hello, quotes are here!\"' }  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    String text = doc.field("text");
    assertEquals("Hello, quotes are here!", text);
  }

  @Test
  public void testStringStartedFromDigit() throws Exception {
    String cfgJson = "{source: { content: { value: 'address\n\"401 Congress Ave, Suite 2450\"' }  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    String address = doc.field("address");
    assertEquals("401 Congress Ave, Suite 2450", address);
  }

  @Test
  public void testFloat() {
    String cfgJson = "{source: { content: { value: 'firstNumber\n10.78'}  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(10.78f, doc.field("firstNumber"));
  }

  @Test
  public void testFloatWithinQuotes() {
    String cfgJson = "{source: { content: { value: 'firstNumber\n\"10.78\"'}  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(10.78f, doc.field("firstNumber"));
  }

  @Test
  public void testFloatWithinQuotesAndCommaAsDecimalSeparator() {
    String cfgJson = "{source: { content: { value: 'firstNumber\n\"10,78\"'}  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(10.78f, doc.field("firstNumber"));
  }

  @Test
  public void testDouble() {
    Double minDouble = 540282346638528870000000000000000000000.0d;

    String cfgJson = "{source: { content: { value: 'secondNumber\n540282346638528870000000000000000000000.0'}  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(minDouble, (Double) doc.field("secondNumber"));
  }

  @Test
  public void testDoubleWithingQuotes() {
    Double minDouble = 540282346638528870000000000000000000000.0d;

    String cfgJson = "{source: { content: { value: 'secondNumber\n\"540282346638528870000000000000000000000.0\"'}  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(minDouble, (Double) doc.field("secondNumber"));
  }

  @Test
  public void testDoubleWithingQuotesAndCommaAsDecimalSeparator() {
    Double minDouble = 540282346638528870000000000000000000000.0d;

    String cfgJson = "{source: { content: { value: 'secondNumber\n\"540282346638528870000000000000000000000,0\"'}  }, extractor : { row: {} }, transformers : [{ csv: {} }], loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(minDouble, (Double) doc.field("secondNumber"));
  }

  @Test
  public void testInteger() {
    String cfgJson = "{source: { content: { value: 'number\n100'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(100), (Integer) doc.field("number"));
  }

  @Test
  public void testIntegerWithingQuotes() {
    String cfgJson = "{source: { content: { value: 'number\n\"100\"'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(100), (Integer) doc.field("number"));
  }

  @Test
  public void testLong() {
    String cfgJson = "{source: { content: { value: 'number\n3000000000'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Long(3000000000L), (Long) doc.field("number"));
  }

  @Test
  public void testLongWithingQuotes() {
    String cfgJson = "{source: { content: { value: 'number\n\"3000000000\"'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Long(3000000000L), (Long) doc.field("number"));
  }

  @Test
  public void testGetCellContentSingleQuoted() {
    String singleQuotedString = "\"aaa\"";
    String unQuotedString = "aaa";
    OCSVTransformer ocsvTransformer = new OCSVTransformer();
    assertEquals(unQuotedString, ocsvTransformer.getCellContent(singleQuotedString));
  }

  @Test
  public void testGetCellContentDoubleQuoted() {
    String doubleQuotedString = "\"\"aaa\"\"";
    String unQuotedString = "\"aaa\"";
    OCSVTransformer ocsvTransformer = new OCSVTransformer();
    assertEquals(unQuotedString, ocsvTransformer.getCellContent(doubleQuotedString));
  }

  @Test
  public void testGetCellContentNullValue() {
    OCSVTransformer ocsvTransformer = new OCSVTransformer();
    assertEquals(null, ocsvTransformer.getCellContent(null));
  }

  @Test
  public void testGetCellContentWithoutQuoteString() {
    String unQuotedString = "aaa";
    OCSVTransformer ocsvTransformer = new OCSVTransformer();
    assertEquals(unQuotedString, ocsvTransformer.getCellContent(unQuotedString));
  }

  @Test
  public void testIsFiniteFloat() {
    OCSVTransformer ocsvTransformer = new OCSVTransformer();
    assertFalse(ocsvTransformer.isFinite(Float.NaN));
    assertFalse(ocsvTransformer.isFinite(Float.POSITIVE_INFINITY));
    assertFalse(ocsvTransformer.isFinite(Float.NEGATIVE_INFINITY));
    assertTrue(ocsvTransformer.isFinite(0f));
  }

  @Test
  public void testNullCell() {
    String cfgJson = "{source: { content: { value: 'id,postId,text\n1,,Hello'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertNull(doc.field("postId"));
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testNullValueInCell() {
    String cfgJson = "{source: { content: { value: 'id,postId,text\n1,NULL,Hello'} }, extractor : { row : {} }, transformers : [{ csv : {nullValue: 'NULL'} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertNull(doc.field("postId"));
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testNullValueInCellEmptyString() {
    String cfgJson = "{source: { content: { value: 'id,title,text\n1,\"\",Hello'} }, extractor : { row : {} }, transformers : [{ csv : {nullValue: 'NULL'} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertEquals("", (String) doc.field("title"));
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testQuotedEmptyString() {
    String cfgJson = "{source: { content: { value: 'id,title,text\n1,\"\",Hello'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertEquals("", (String) doc.field("title"));
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testCRLFDelimiter() {
    String cfgJson = "{source: { content: { value: 'id,text,num\r\n1,my test text,1'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);

    assertThat(doc.<Integer> field("id")).isEqualTo(1);
    assertThat(doc.<String> field("text")).isEqualTo("my test text");
    assertThat(doc.<Integer> field("num")).isEqualTo(1);
  }

  @Test
  public void testEndingLineBreak() {
    String cfgJson = "{source: { content: { value: 'id,text,num\r\n1,my test text,1\r\n'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertEquals("my test text", (String) doc.field("text"));
    assertEquals(new Integer(1), (Integer) doc.field("num"));
  }

  @Test
  public void testEndingSpaceInFieldName() {
    String cfgJson = "{source: { content: { value: 'id ,text ,num \r\n1,my test text,1\r\n'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat(doc.<Integer> field("id ")).isEqualTo(1);
    assertThat(doc.field("text")).isNull();
    assertThat(doc.<String> field("text ")).isEqualTo("my test text");
    assertThat(doc.<Integer> field("num ")).isEqualTo(1);
  }

  @Test
  public void testCRLFIWithinQuotes() {
    String cfgJson = "{source: { content: { value: 'id ,text ,num \r\n1,\"my test\r\n text\",1\r\n'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat((Integer) doc.field("id ")).isEqualTo(1);
    assertThat((String) doc.field("text ")).isEqualTo("my test\r\n text");
    assertThat((Integer) doc.field("num ")).isEqualTo(1);
  }

  @Test
  public void testEscapingDoubleQuotes() {
    String cfgJson = "{source: { content: { value: 'id ,text ,num \r\n1,\"my test \"\" text\",1\r\n'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat(doc.<Integer> field("id ")).isEqualTo(1);
    assertThat(doc.<String> field("text ")).isEqualTo("my test \"\" text");
    assertThat(doc.<Integer> field("num ")).isEqualTo(1);
  }

  public void testNegativeInteger() {
    String cfgJson = "{source: { content: { value: 'id\r\n-1'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat(doc.<Integer> field("id")).isEqualTo(-1);

  }

  public void testNegativeFloat() {
    String cfgJson = "{source: { content: { value: 'id\r\n-1.0'} }, extractor : { row : {} }, transformers : [{ csv : {} }], loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat(doc.<Float> field("id")).isEqualTo(-1.0f);
  }
}
