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

import com.orientechnologies.orient.core.sql.parser.OContainsCondition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OContainsConditionTest {
  @Test
  public void test() {
    OContainsCondition op = new OContainsCondition(-1);

    Assert.assertFalse(op.execute(null, null));
    Assert.assertFalse(op.execute(null, "foo"));

    List<Object> left = new ArrayList<Object>();
    Assert.assertFalse(op.execute(left, "foo"));
    Assert.assertFalse(op.execute(left, null));

    left.add("foo");
    left.add("bar");

    Assert.assertTrue(op.execute(left, "foo"));
    Assert.assertTrue(op.execute(left, "bar"));
    Assert.assertFalse(op.execute(left, "fooz"));

    left.add(null);
    Assert.assertTrue(op.execute(left, null));
  }

  @Test
  public void testIterable() {
    Iterable left =
        new Iterable() {
          private final List<Integer> ls = Arrays.asList(3, 1, 2);

          @Override
          public Iterator iterator() {
            return ls.iterator();
          }
        };

    Iterable right =
        new Iterable() {
          private final List<Integer> ls = Arrays.asList(2, 3);

          @Override
          public Iterator iterator() {
            return ls.iterator();
          }
        };

    OContainsCondition op = new OContainsCondition(-1);
    Assert.assertTrue(op.execute(left, right));
  }
}
