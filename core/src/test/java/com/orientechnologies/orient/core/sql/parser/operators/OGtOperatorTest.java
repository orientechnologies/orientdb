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

import com.orientechnologies.orient.core.sql.parser.OGtOperator;
import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OGtOperatorTest {
  @Test
  public void test() {
    OGtOperator op = new OGtOperator(-1);
    Assert.assertFalse(op.execute(1, 1));
    Assert.assertTrue(op.execute(1, 0));
    Assert.assertFalse(op.execute(0, 1));

    Assert.assertFalse(op.execute("aaa", "zzz"));
    Assert.assertTrue(op.execute("zzz", "aaa"));

    Assert.assertFalse(op.execute("aaa", "aaa"));

    Assert.assertFalse(op.execute(1, 1.1));
    Assert.assertTrue(op.execute(1.1, 1));

    Assert.assertFalse(op.execute(BigDecimal.ONE, 1));
    Assert.assertFalse(op.execute(1, BigDecimal.ONE));

    Assert.assertFalse(op.execute(1.1, 1.1));
    Assert.assertFalse(op.execute(new BigDecimal(15), new BigDecimal(15)));

    Assert.assertTrue(op.execute(1.1, BigDecimal.ONE));
    Assert.assertTrue(op.execute(2, BigDecimal.ONE));

    Assert.assertTrue(op.execute(BigDecimal.ONE, 0.999999));
    Assert.assertTrue(op.execute(BigDecimal.ONE, 0));

    Assert.assertFalse(op.execute(BigDecimal.ONE, 2));
    Assert.assertFalse(op.execute(BigDecimal.ONE, 1.0001));

    Assert.assertFalse(op.execute(new Object(), new Object()));
  }
}
