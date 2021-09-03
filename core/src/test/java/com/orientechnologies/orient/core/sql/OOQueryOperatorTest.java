package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorAnd;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContains;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsAll;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsKey;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsText;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsValue;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorInstanceof;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIs;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorLike;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMatches;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNot;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNotEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNotEquals2;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorOr;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorTraverse;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorDivide;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMinus;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMod;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMultiply;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorPlus;
import org.junit.Assert;
import org.junit.Test;

public class OOQueryOperatorTest {

  @Test
  public void testOperatorOrder() {

    // check operator are the correct order
    final OQueryOperator[] operators = OSQLEngine.INSTANCE.getRecordOperators();

    int i = 0;
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorEquals);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorAnd);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorOr);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorNotEquals);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorNotEquals2);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorNot);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMinorEquals);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMinor);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMajorEquals);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorContainsAll);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMajor);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorLike);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMatches);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorInstanceof);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorIs);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorIn);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorContainsKey);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorContainsValue);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorContainsText);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorContains);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorTraverse);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorBetween);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorPlus);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMinus);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMultiply);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorDivide);
    Assert.assertTrue(operators[i++] instanceof OQueryOperatorMod);
  }
}
