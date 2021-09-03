/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "internal")
public class StringsTest {

  @Test
  public void splitArray() {
    List<String> pieces =
        OStringSerializerHelper.smartSplit(
            "first, orders : ['this is mine', 'that is your']",
            new char[] {','},
            0,
            -1,
            true,
            true,
            false,
            false,
            ' ',
            '\n',
            '\r',
            '\t');
    Assert.assertEquals(pieces.size(), 2);
    Assert.assertTrue(pieces.get(1).contains("this is mine"));
  }

  public void replaceAll() {
    String test1 = "test string number 1";
    String test2 =
        "test \\string\\ \"number\" \\2\\ \\\\ \"\"\"\" test String number 2 test string number 2";
    Assert.assertEquals(OStringParser.replaceAll(test1, "", ""), test1);
    Assert.assertEquals(OStringParser.replaceAll(test1, "1", "10"), test1 + "0");
    Assert.assertEquals(
        OStringParser.replaceAll(test1, "string", "number"), "test number number 1");
    Assert.assertEquals(OStringParser.replaceAll(test1, "string", "test"), "test test number 1");
    Assert.assertEquals(
        OStringParser.replaceAll(test1, "test", "string"), "string string number 1");
    Assert.assertEquals(OStringParser.replaceAll(test2, "", ""), test2);
    Assert.assertEquals(
        OStringParser.replaceAll(test2, "\\", ""),
        "test string \"number\" 2  \"\"\"\" test String number 2 test string number 2");
    Assert.assertEquals(
        OStringParser.replaceAll(test2, "\"", "'"),
        "test \\string\\ 'number' \\2\\ \\\\ '''' test String number 2 test string number 2");
    Assert.assertEquals(
        OStringParser.replaceAll(test2, "\\\\", "replacement"),
        "test \\string\\ \"number\" \\2\\ replacement \"\"\"\" test String number 2 test string number 2");
    String subsequentReplaceTest = OStringParser.replaceAll(test2, "\\", "");
    subsequentReplaceTest = OStringParser.replaceAll(subsequentReplaceTest, "\"", "");
    subsequentReplaceTest =
        OStringParser.replaceAll(
            subsequentReplaceTest, "test string number 2", "text replacement 1");
    Assert.assertEquals(
        subsequentReplaceTest, "text replacement 1   test String number 2 text replacement 1");
  }

  public void testNoEmptyFields() {
    List<String> pieces =
        OStringSerializerHelper.split(
            "1811000032;03/27/2014;HA297000610K;+3415.4000;+3215.4500;+0.0000;+1117.0000;+916.7500;3583;890;+64.8700;4;4;+198.0932",
            ';');
    Assert.assertEquals(pieces.size(), 14);
  }

  public void testEmptyFields() {
    List<String> pieces =
        OStringSerializerHelper.split(
            "1811000032;03/27/2014;HA297000960C;+0.0000;+0.0000;+0.0000;+0.0000;+0.0000;0;0;+0.0000;;5;+0.0000",
            ';');
    Assert.assertEquals(pieces.size(), 14);
  }

  public void testDocumentSelfReference() {
    ODocument document = new ODocument();
    document.field("selfref", document);

    ODocument docTwo = new ODocument();
    docTwo.field("ref", document);
    document.field("ref", docTwo);

    String value = document.toString();

    Assert.assertEquals(value, "{selfref:<recursion:rid=#-1:-1>,ref:{ref:<recursion:rid=#-1:-1>}}");
  }
}
