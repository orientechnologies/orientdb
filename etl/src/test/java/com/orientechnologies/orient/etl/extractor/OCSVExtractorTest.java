package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.ETLBaseTest;
import com.orientechnologies.orient.etl.transformer.OCSVTransformer;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Created by frank on 10/5/15.
 */
public class OCSVExtractorTest extends ETLBaseTest {

  @Test
  public void testOneObject() {
    process("{source: { content: { value: 'name,surname\nJay,Miner' } }, extractor : { csv: {} }, loader: { test: {} } }");

    assertEquals(1, getResult().size());
    ODocument doc = getResult().get(0);
    assertEquals(2, doc.fields());
    assertEquals("Jay", doc.field("name"));
    assertEquals("Miner", doc.field("surname"));
  }

  @Test
  public void testEmpty() {
    String cfgJson = "{source: { content: { value: '' }  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    assertEquals(0, getResult().size());
  }

  @Test
  public void testSmallSet() {
    String content = "name,surname,id";
    for (int i = 0; i < names.length; ++i)
      content += "\n" + names[i] + "," + surnames[i] + "," + i;
    process("{source: { content: { value: '" + content + "' } }, extractor : { csv: {} },  loader: { test: {} } }");

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
  public void testSkipFromTwoToFour() {
    String content = "name,surname,id";
    for (int i = 0; i < names.length; ++i)
      content += "\n" + names[i] + "," + surnames[i] + "," + i;
    process("{source: { content: { value: '" + content + "' } }, " + "extractor : { csv: {skipFrom: 1, skipTo: 4} },  "
        + "loader: { test: {} } }");

    assertThat(getResult()).hasSize(names.length - 4);
  }

  @Test
  public void testDateTypeAutodetection() {
    String cfgJson = "{source: { content: { value: 'BirthDay\n2008-04-30' }  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    Date birthday = doc.field("BirthDay");
    assertEquals(2008, birthday.getYear() + 1900);
    assertEquals(4, birthday.getMonth() + 1);
    assertEquals(30, birthday.getDate());
  }

  @Test
  public void testDateTypeAutodetectionWithCustomDateFormat() {
    String cfgJson = "{source: { content: { value: 'BirthDay\n30-04-2008' }  }, extractor : { csv: {dateFormat: \"dd-MM-yyyy\"} }, loader: { test: {} } }";
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
    String cfgJson = "{source: { content: { value: 'text\n\"Hello, quotes are here!\"' }  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    String text = doc.field("text");
    assertEquals("Hello, quotes are here!", text);
  }

  @Test
  public void testStringStartedFromDigit() throws Exception {
    String cfgJson = "{source: { content: { value: 'address\n\"401 Congress Ave, Suite 2450\"' }  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    String address = doc.field("address");
    assertEquals("401 Congress Ave, Suite 2450", address);
  }

  @Test
  public void testFloat() {
    String cfgJson = "{source: { content: { value: 'firstNumber\n10.78'}  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(10.78f, doc.field("firstNumber"));
  }

  @Test
  public void testFloatWithinQuotes() {
    String cfgJson = "{source: { content: { value: 'firstNumber\n\"10.78\"'}  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(10.78f, doc.field("firstNumber"));
  }

  @Test
  public void testFloatWithinQuotesAndCommaAsDecimalSeparator() {
    String cfgJson = "{source: { content: { value: 'firstNumber\n\"10,78\"'}  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(10.78f, doc.field("firstNumber"));
  }

  @Test
  public void testDouble() {
    Double minDouble = 540282346638528870000000000000000000000.0d;

    String cfgJson = "{source: { content: { value: 'secondNumber\n540282346638528870000000000000000000000.0'}  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(minDouble, (Double) doc.field("secondNumber"));
  }

  @Test
  public void testDoubleWithingQuotes() {
    Double minDouble = 540282346638528870000000000000000000000.0d;

    String cfgJson = "{source: { content: { value: 'secondNumber\n\"540282346638528870000000000000000000000.0\"'}  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(minDouble, (Double) doc.field("secondNumber"));
  }

  @Test
  public void testDoubleWithingQuotesAndCommaAsDecimalSeparator() {
    Double minDouble = 540282346638528870000000000000000000000.0d;

    String cfgJson = "{source: { content: { value: 'secondNumber\n\"540282346638528870000000000000000000000,0\"'}  }, extractor : { csv: {} }, loader: { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(minDouble, (Double) doc.field("secondNumber"));
  }

  @Test
  public void testInteger() {
    String cfgJson = "{source: { content: { value: 'number\n100'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(100), (Integer) doc.field("number"));
  }

  @Test
  public void testIntegerWithingQuotes() {
    String cfgJson = "{source: { content: { value: 'number\n\"100\"'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(100), (Integer) doc.field("number"));
  }

  @Test
  public void testLong() {
    String cfgJson = "{source: { content: { value: 'number\n3000000000'} }, extractor : { csv: {} },  loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Long(3000000000L), (Long) doc.field("number"));
  }

  @Test
  public void testLongWithingQuotes() {
    String cfgJson = "{source: { content: { value: 'number\n\"3000000000\"'} }, extractor : { csv : {} }, loader : { test: {} } }";
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
  public void testIsFiniteFloat() {
    OCSVExtractor ocsvExtractor = new OCSVExtractor();
    assertFalse(ocsvExtractor.isFinite(Float.NaN));
    assertFalse(ocsvExtractor.isFinite(Float.POSITIVE_INFINITY));
    assertFalse(ocsvExtractor.isFinite(Float.NEGATIVE_INFINITY));
    assertTrue(ocsvExtractor.isFinite(0f));
  }

  @Test
  public void testNullCell() {
    String cfgJson = "{source: { content: { value: 'id,postId,text\n1,,Hello'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertNull(doc.field("postId"));
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testNullValueInCell() {
    String cfgJson = "{source: { content: { value: 'id,postId,text\n1,NULL,Hello'} }, extractor : { csv : {} },  loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertNull(doc.field("postId"));
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testNullValueInCellEmptyString() {
    String cfgJson = "{source: { content: { value: 'id,title,text\n1,\"\",Hello'} }, extractor : { csv: {} },  loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertThat(doc.field("title")).isNull();
    // assertEquals("", (String) doc.field("title"));
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testQuotedEmptyString() {
    String cfgJson = "{source: { content: { value: 'id,title,text\n1,\"\",Hello'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertThat(doc.field("title")).isNull();
    assertEquals("Hello", (String) doc.field("text"));
  }

  @Test
  public void testCRLFDelimiter() {
    String cfgJson = "{source: { content: { value: 'id,text,num\r\n1,my test text,1'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);

    assertThat(doc.<Integer> field("id")).isEqualTo(1);
    assertThat(doc.<String> field("text")).isEqualTo("my test text");
    assertThat(doc.<Integer> field("num")).isEqualTo(1);
  }

  @Test
  public void testEndingLineBreak() {
    String cfgJson = "{source: { content: { value: 'id,text,num\r\n1,my test text,1\r\n'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertEquals(new Integer(1), (Integer) doc.field("id"));
    assertEquals("my test text", (String) doc.field("text"));
    assertEquals(new Integer(1), (Integer) doc.field("num"));
  }

  @Test
  public void testEndingSpaceInFieldName() {
    String cfgJson = "{source: { content: { value: 'id ,text ,num \r\n1,my test text,1\r\n'} }, extractor : { csv : {} }, loader : { test: {} } }";
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
    String cfgJson = "{source: { content: { value: 'id ,text ,num \r\n1,\"my test\r\n text\",1\r\n'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat((Integer) doc.field("id ")).isEqualTo(1);
    assertThat((String) doc.field("text ")).isEqualTo("my test\r\n text");
    assertThat((Integer) doc.field("num ")).isEqualTo(1);
  }

  @Test
  public void testEscapingDoubleQuotes() {
    String cfgJson = "{source: { content: { value: 'id ,text ,num \r\n1,my test \"\" text,1\r\n'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat(doc.<Integer> field("id ")).isEqualTo(1);
    assertThat(doc.<String> field("text ")).isEqualTo("my test \"\" text");
    assertThat(doc.<Integer> field("num ")).isEqualTo(1);
  }

  public void testNegativeInteger() {
    String cfgJson = "{source: { content: { value: 'id\r\n-1'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat(doc.<Integer> field("id")).isEqualTo(-1);

  }

  public void testNegativeFloat() {
    String cfgJson = "{source: { content: { value: 'id\r\n-1.0'} }, extractor : { csv : {} }, loader : { test: {} } }";
    process(cfgJson);
    List<ODocument> res = getResult();
    ODocument doc = res.get(0);
    assertThat(doc.<Float> field("id")).isEqualTo(-1.0f);
  }

}
