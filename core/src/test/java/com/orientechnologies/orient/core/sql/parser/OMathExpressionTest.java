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
package com.orientechnologies.orient.core.sql.parser;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * Created by luigidellaquila on 02/07/15.
 */
public class OMathExpressionTest {

  @Test
  public void testTypes() {

    OMathExpression expr = new OMathExpression(-1);

    for (OMathExpression.Operator op : OMathExpression.Operator.values()) {
      Assert.assertEquals(expr.apply(1, op, 1).getClass(), Integer.class);

      Assert.assertEquals(expr.apply((short) 1, op, (short) 1).getClass(), Integer.class);

      Assert.assertEquals(expr.apply(1l, op, 1l).getClass(), Long.class);
      Assert.assertEquals(expr.apply(1f, op, 1f).getClass(), Float.class);
      Assert.assertEquals(expr.apply(1d, op, 1d).getClass(), Double.class);
      Assert.assertEquals(expr.apply(BigDecimal.ONE, op, BigDecimal.ONE).getClass(), BigDecimal.class);

      Assert.assertEquals(expr.apply(1l, op, 1).getClass(), Long.class);
      Assert.assertEquals(expr.apply(1f, op, 1).getClass(), Float.class);
      Assert.assertEquals(expr.apply(1d, op, 1).getClass(), Double.class);
      Assert.assertEquals(expr.apply(BigDecimal.ONE, op, 1).getClass(), BigDecimal.class);

      Assert.assertEquals(expr.apply(1, op, 1l).getClass(), Long.class);
      Assert.assertEquals(expr.apply(1, op, 1f).getClass(), Float.class);
      Assert.assertEquals(expr.apply(1, op, 1d).getClass(), Double.class);
      Assert.assertEquals(expr.apply(1, op, BigDecimal.ONE).getClass(), BigDecimal.class);
    }

    Assert.assertEquals(expr.apply(Integer.MAX_VALUE, OMathExpression.Operator.PLUS, 1).getClass(), Long.class);
    Assert.assertEquals(expr.apply(Integer.MIN_VALUE, OMathExpression.Operator.MINUS, 1).getClass(), Long.class);
  }

}
