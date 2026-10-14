/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.parser.operators;

import com.orientechnologies.orient.core.sql.parser.OLikeOperator;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OLikeOperatorTest {
  @Test
  public void test() {
    OLikeOperator op = new OLikeOperator(-1);
    Assert.assertTrue(op.execute("foobar", "%ooba%", null));
    Assert.assertTrue(op.execute("foobar", "%oo%", null));
    Assert.assertFalse(op.execute("foobar", "oo%", null));
    Assert.assertFalse(op.execute("foobar", "%oo", null));
    Assert.assertFalse(op.execute("foobar", "%fff%", null));
    Assert.assertTrue(op.execute("foobar", "foobar", null));
  }

  @Test
  public void testLineTerminators() {
    OLikeOperator op = new OLikeOperator(-1);

    String[] values = {"a\nb", "a\rb", "a\tb", "ab"};
    for (String value : values) {
      Assert.assertTrue(op.execute(value, "a%", null));
      Assert.assertTrue(op.execute(value, "%b", null));
      Assert.assertTrue(op.execute(value, "%a%", null));
    }
    Assert.assertTrue(op.execute("a\nb", "a?b", null));
    Assert.assertTrue(op.execute("a\rb", "a?b", null));
    Assert.assertTrue(op.execute("a\tb", "a?b", null));

    String[] terminators = {"a\u0085b", "a\u2028b", "a\u2029b"};
    for (String value : terminators) {
      Assert.assertTrue(op.execute(value, "a%", null));
      Assert.assertTrue(op.execute(value, "%b", null));
      Assert.assertTrue(op.execute(value, "a?b", null));
    }

    Assert.assertTrue(op.execute("a\r\nb", "a??b", null));
    Assert.assertFalse(op.execute("a\r\nb", "a?b", null));
    Assert.assertTrue(op.execute("\n", "%", null));
    Assert.assertTrue(op.execute("\n", "?", null));

    Assert.assertTrue(op.execute("A\nB", "a%b", null));
    Assert.assertTrue(op.execute("A\nB", "a?b", null));
    Assert.assertFalse(op.execute("a\nb", "ab", null));

    Assert.assertTrue(op.execute("a[\nb", "a[%", null));
    Assert.assertFalse(op.execute("a[\nb", "a[b", null));

    Assert.assertFalse(op.execute(null, "%", null));
    Assert.assertFalse(op.execute("a\nb", null, null));
    Assert.assertFalse(op.execute("", "%", null));
    Assert.assertFalse(op.execute("a\nb", "", null));
  }
}
