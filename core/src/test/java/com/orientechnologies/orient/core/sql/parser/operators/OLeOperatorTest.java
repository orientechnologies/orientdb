/*
  *
  *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
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

import com.orientechnologies.orient.core.sql.parser.OLeOperator;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;

/**
 * @author Luigi Dell'Aquila
 */
public class OLeOperatorTest {
  @Test
  public void test() {
    OLeOperator op = new OLeOperator(-1);
    Assert.assertTrue(op.execute(1, 1));
    Assert.assertFalse(op.execute(1, 0));
    Assert.assertTrue(op.execute(0, 1));

    Assert.assertTrue(op.execute("aaa", "zzz"));
    Assert.assertFalse(op.execute("zzz", "aaa"));

    Assert.assertTrue(op.execute(1, 1.1));
    Assert.assertFalse(op.execute(1.1, 1));

    Assert.assertTrue(op.execute(BigDecimal.ONE, 1));
    Assert.assertTrue(op.execute(1, BigDecimal.ONE));

    Assert.assertFalse(op.execute(1.1, BigDecimal.ONE));
    Assert.assertFalse(op.execute(2, BigDecimal.ONE));

    Assert.assertFalse(op.execute(BigDecimal.ONE, 0.999999));
    Assert.assertFalse(op.execute(BigDecimal.ONE, 0));

    Assert.assertTrue(op.execute(BigDecimal.ONE, 2));
    Assert.assertTrue(op.execute(BigDecimal.ONE, 1.0001));
    try {
      Assert.assertFalse(op.execute(new Object(), new Object()));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof ClassCastException);
    }
  }
}
