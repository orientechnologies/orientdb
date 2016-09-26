package com.orientechnologies.orient.core.sql;


import com.orientechnologies.orient.core.sql.operator.*;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorDivide;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMinus;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMod;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMultiply;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorPlus;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class OOQueryOperatorTest {

    @Test
    public void testOperatorOrder() {
        
        //check operator are the correct order
        final OQueryOperator[] operators = OSQLEngine.INSTANCE.getRecordOperators();
        
        int i=0;
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
