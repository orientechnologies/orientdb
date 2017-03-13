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
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by luigidellaquila on 02/07/15.
 */
public class OMathExpressionTest {

  @Test
  public void testTypes() {

    OMathExpression expr = new OMathExpression(-1);

    OMathExpression.Operator[] basicOps = new OMathExpression.Operator[] { OMathExpression.Operator.PLUS,
        OMathExpression.Operator.MINUS, OMathExpression.Operator.STAR, OMathExpression.Operator.SLASH,
        OMathExpression.Operator.REM };

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

    Assert.assertEquals(OMathExpression.Operator.PLUS.apply(Integer.MAX_VALUE, 1).getClass(), Long.class);
    Assert.assertEquals(OMathExpression.Operator.MINUS.apply(Integer.MIN_VALUE, 1).getClass(), Long.class);
  }

  @Test
  public void testPriority() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(10));
    exp.operators.add(OMathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(5));
    exp.operators.add(OMathExpression.Operator.STAR);
    exp.childExpressions.add(integer(8));
    exp.operators.add(OMathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(2));
    exp.operators.add(OMathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(OMathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(208, result);

  }

  @Test
  public void testPriority2() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(1));
    exp.operators.add(OMathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(2));
    exp.operators.add(OMathExpression.Operator.STAR);
    exp.childExpressions.add(integer(3));
    exp.operators.add(OMathExpression.Operator.STAR);
    exp.childExpressions.add(integer(4));
    exp.operators.add(OMathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(8));
    exp.operators.add(OMathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(2));
    exp.operators.add(OMathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(1));
    exp.operators.add(OMathExpression.Operator.MINUS);
    exp.childExpressions.add(integer(3));
    exp.operators.add(OMathExpression.Operator.PLUS);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(16, result);
  }

  @Test
  public void testPriority3() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(3));
    exp.operators.add(OMathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(OMathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(2, result);
  }

  @Test
  public void testPriority4() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(3));
    exp.operators.add(OMathExpression.Operator.LSHIFT);
    exp.childExpressions.add(integer(1));
    exp.operators.add(OMathExpression.Operator.RSHIFT);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(3, result);
  }

  @Test
  public void testAnd() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(5));
    exp.operators.add(OMathExpression.Operator.BIT_AND);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(1, result);
  }

  @Test
  public void testAnd2() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(5));
    exp.operators.add(OMathExpression.Operator.BIT_AND);
    exp.childExpressions.add(integer(4));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(4, result);
  }

  @Test
  public void testOr() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(4));
    exp.operators.add(OMathExpression.Operator.BIT_OR);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof Integer);
    Assert.assertEquals(5, result);
  }

  @Test
  public void testArrayConcat() {
    OMathExpression exp = new OMathExpression(-1);
    exp.childExpressions.add(integer(4));
    exp.operators.add(OMathExpression.Operator.SC_OR);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof List);
    List list = (List) result;
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(4, list.get(0));
    Assert.assertEquals(1, list.get(1));
  }

  @Test
  public void testArrayConcat2() {
    OMathExpression exp = new OMathExpression(-1);

    exp.childExpressions.add(list(5, 19));
    exp.operators.add(OMathExpression.Operator.SC_OR);
    exp.childExpressions.add(integer(1));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof List);
    List list = (List) result;
    Assert.assertEquals(3, list.size());
    Assert.assertEquals(5, list.get(0));
    Assert.assertEquals(19, list.get(1));
    Assert.assertEquals(1, list.get(2));
  }

  @Test
  public void testArrayConcat3() {
    OMathExpression exp = new OMathExpression(-1);

    exp.childExpressions.add(integer(1));
    exp.operators.add(OMathExpression.Operator.SC_OR);
    exp.childExpressions.add(list(5, 19));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof List);
    List list = (List) result;
    Assert.assertEquals(3, list.size());
    Assert.assertEquals(1, list.get(0));
    Assert.assertEquals(5, list.get(1));
    Assert.assertEquals(19, list.get(2));
  }


  @Test
  public void testArrayConcat4() {
    OMathExpression exp = new OMathExpression(-1);

    exp.childExpressions.add(list(1, 8));
    exp.operators.add(OMathExpression.Operator.SC_OR);
    exp.childExpressions.add(list(5, 19));

    Object result = exp.execute((OResult) null, null);
    Assert.assertTrue(result instanceof List);
    List list = (List) result;
    Assert.assertEquals(4, list.size());
    Assert.assertEquals(1, list.get(0));
    Assert.assertEquals(8, list.get(1));
    Assert.assertEquals(5, list.get(2));
    Assert.assertEquals(19, list.get(3));
  }

  private OMathExpression integer(Number i) {
    OBaseExpression exp = new OBaseExpression(-1);
    OInteger integer = new OInteger(-1);
    integer.setValue(i);
    exp.number = integer;
    return exp;
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

}
