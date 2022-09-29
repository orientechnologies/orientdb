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

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.parser.OMathExpression.Operator;
import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 02/07/15. */
public class OMathExpressionTest {

  @Test
  public void testTypes() {

    OMathExpression expr = new OMathExpression(-1);

    OMathExpression.Operator[] basicOps =
        new OMathExpression.Operator[] {
          OMathExpression.Operator.PLUS,
          OMathExpression.Operator.MINUS,
          OMathExpression.Operator.STAR,
          OMathExpression.Operator.SLASH,
          OMathExpression.Operator.REM
        };

    for (OMathExpression.Operator op : basicOps) {
      Assert.assertEquals(op.apply(1, 1).getClass(), Integer.class);

      Assert.assertEquals(op.apply((short) 1, (short) 1).getClass(), Integer.class);

      Assert.assertEquals(op.apply(1l, 1l).getClass(), Long.class);
      Assert.assertEquals(op.apply(1f, 1f).getClass(), Float.class);
      Assert.assertEquals(op.apply(1d, 1d).getClass(), Double.class);
      Assert.assertEquals(op.apply(BigDecimal.ONE, BigDecimal.ONE).getClass(), BigDecimal.class);

      Assert.assertEquals(op.apply(1l, 1).getClass(), Long.class);
      Assert.assertEquals(op.apply(1f, 1).getClass(), Float.class);
      Assert.assertEquals(op.apply(1d, 1).getClass(), Double.class);
      Assert.assertEquals(op.apply(BigDecimal.ONE, 1).getClass(), BigDecimal.class);

      Assert.assertEquals(op.apply(1, 1l).getClass(), Long.class);
      Assert.assertEquals(op.apply(1, 1f).getClass(), Float.class);
      Assert.assertEquals(op.apply(1, 1d).getClass(), Double.class);
      Assert.assertEquals(op.apply(1, BigDecimal.ONE).getClass(), BigDecimal.class);
    }

    Assert.assertEquals(
        OMathExpression.Operator.PLUS.apply(Integer.MAX_VALUE, 1).getClass(), Long.class);
    Assert.assertEquals(
        OMathExpression.Operator.MINUS.apply(Integer.MIN_VALUE, 1).getClass(), Long.class);
  }

  @Test
  public void testPriority() {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(integer(10));
    exp.addOperator(OMathExpression.Operator.PLUS);
    exp.addChildExpression(integer(5));
    exp.addOperator(OMathExpression.Operator.STAR);
    exp.addChildExpression(integer(8));
    exp.addOperator(OMathExpression.Operator.PLUS);
    exp.addChildExpression(integer(2));
    exp.addOperator(OMathExpression.Operator.LSHIFT);
    exp.addChildExpression(integer(1));
    exp.addOperator(OMathExpression.Operator.PLUS);
    exp.addChildExpression(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(208, result);
  }

  @Test
  public void testPriority2() {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(integer(1));
    exp.addOperator(OMathExpression.Operator.PLUS);
    exp.addChildExpression(integer(2));
    exp.addOperator(OMathExpression.Operator.STAR);
    exp.addChildExpression(integer(3));
    exp.addOperator(OMathExpression.Operator.STAR);
    exp.addChildExpression(integer(4));
    exp.addOperator(OMathExpression.Operator.PLUS);
    exp.addChildExpression(integer(8));
    exp.addOperator(OMathExpression.Operator.RSHIFT);
    exp.addChildExpression(integer(2));
    exp.addOperator(OMathExpression.Operator.PLUS);
    exp.addChildExpression(integer(1));
    exp.addOperator(OMathExpression.Operator.MINUS);
    exp.addChildExpression(integer(3));
    exp.addOperator(OMathExpression.Operator.PLUS);
    exp.addChildExpression(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(16, result);
  }

  @Test
  public void testPriority3() {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(integer(3));
    exp.addOperator(OMathExpression.Operator.RSHIFT);
    exp.addChildExpression(integer(1));
    exp.addOperator(OMathExpression.Operator.LSHIFT);
    exp.addChildExpression(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(2, result);
  }

  @Test
  public void testPriority4() {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(integer(3));
    exp.addOperator(OMathExpression.Operator.LSHIFT);
    exp.addChildExpression(integer(1));
    exp.addOperator(OMathExpression.Operator.RSHIFT);
    exp.addChildExpression(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(3, result);
  }

  @Test
  public void testAnd() {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(integer(5));
    exp.addOperator(OMathExpression.Operator.BIT_AND);
    exp.addChildExpression(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(1, result);
  }

  @Test
  public void testAnd2() {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(integer(5));
    exp.addOperator(OMathExpression.Operator.BIT_AND);
    exp.addChildExpression(integer(4));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(4, result);
  }

  @Test
  public void testOr() {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(integer(4));
    exp.addOperator(OMathExpression.Operator.BIT_OR);
    exp.addChildExpression(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(5, result);
  }

  private OMathExpression integer(Number i) {
    OBaseExpression exp = new OBaseExpression(-1);
    OInteger integer = new OInteger(-1);
    integer.setValue(i);
    exp.number = integer;
    return exp;
  }

  private OMathExpression str(String value) {
    final OBaseExpression exp = new OBaseExpression(-1);
    exp.string = "'" + value + "'";
    return exp;
  }

  private OMathExpression nullExpr() {
    return new OBaseExpression(-1);
  }

  private OMathExpression list(Number... values) {
    OBaseExpression exp = new OBaseExpression(-1);
    exp.identifier = new OBaseIdentifier(-1);
    exp.identifier.levelZero = new OLevelZeroIdentifier(-1);
    OCollection coll = new OCollection(-1);
    exp.identifier.levelZero.collection = coll;

    for (Number val : values) {
      OExpression sub = new OExpression(-1);
      sub.mathExpression = integer(val);
      coll.expressions.add(sub);
    }
    return exp;
  }

  @Test
  public void testNullCoalescing() {
    testNullCoalescingGeneric(integer(20), integer(15), 20);
    testNullCoalescingGeneric(nullExpr(), integer(14), 14);
    testNullCoalescingGeneric(str("32"), nullExpr(), "32");
    testNullCoalescingGeneric(str("2"), integer(5), "2");
    testNullCoalescingGeneric(nullExpr(), str("3"), "3");
  }

  private void testNullCoalescingGeneric(
      OMathExpression left, OMathExpression right, Object expected) {
    OMathExpression exp = new OMathExpression(-1);
    exp.addChildExpression(left);
    exp.addOperator(Operator.NULL_COALESCING);
    exp.addChildExpression(right);

    Object result = exp.execute((OResult) null, null);
    //    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(expected, result);
  }
}
