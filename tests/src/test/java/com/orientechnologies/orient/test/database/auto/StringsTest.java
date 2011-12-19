/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

@Test(groups = "internal")
public class StringsTest {

	@Test
	public void splitArray() {
		List<String> pieces = OStringSerializerHelper.smartSplit("first, orders : ['this is mine', 'that is your']",
				new char[] { ',' }, 0, -1, true, ' ', '\n', '\r', '\t');
		Assert.assertEquals(pieces.size(), 2);
		Assert.assertTrue(pieces.get(1).contains("this is mine"));
	}

	public void replaceAll() {
		String test1 = "test string number 1";
		String test2 = "test \\string\\ \"number\" \\2\\ \\\\ \"\"\"\" test String number 2 test string number 2";
		Assert.assertEquals(OStringParser.replaceAll(test1, "", ""), test1);
		Assert.assertEquals(OStringParser.replaceAll(test1, "1", "10"), test1 + "0");
		Assert.assertEquals(OStringParser.replaceAll(test1, "string", "number"), "test number number 1");
		Assert.assertEquals(OStringParser.replaceAll(test1, "string", "test"), "test test number 1");
		Assert.assertEquals(OStringParser.replaceAll(test1, "test", "string"), "string string number 1");
		Assert.assertEquals(OStringParser.replaceAll(test2, "", ""), test2);
		Assert.assertEquals(OStringParser.replaceAll(test2, "\\", ""),
				"test string \"number\" 2  \"\"\"\" test String number 2 test string number 2");
		Assert.assertEquals(OStringParser.replaceAll(test2, "\"", "'"),
				"test \\string\\ 'number' \\2\\ \\\\ '''' test String number 2 test string number 2");
		Assert.assertEquals(OStringParser.replaceAll(test2, "\\\\", "replacement"),
				"test \\string\\ \"number\" \\2\\ replacement \"\"\"\" test String number 2 test string number 2");
		String subsequentReplaceTest = OStringParser.replaceAll(test2, "\\", "");
		subsequentReplaceTest = OStringParser.replaceAll(subsequentReplaceTest, "\"", "");
		subsequentReplaceTest = OStringParser.replaceAll(subsequentReplaceTest, "test string number 2", "text replacement 1");
		Assert.assertEquals(subsequentReplaceTest, "text replacement 1   test String number 2 text replacement 1");
	}

}
