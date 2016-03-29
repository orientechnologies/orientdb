/*
  *
  *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorDivide;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;

/**
 * Created by luigidellaquila on 29/03/16.
 */

public class OQueryOperatorDivideTest {
  @Test
  public void test() {
    OQueryOperator operator = new OQueryOperatorDivide();
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, 3, null), 10 / 3);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10l, 3l, null), 10l / 3l);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10.1, 3, null), 10.1 / 3);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, 3.1, null), 10 / 3.1);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10.1d, 3, null), 10.1d / 3);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, 3.1d, null), 10 / 3.1d);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, new BigDecimal(10), 4, null),
        new BigDecimal(10).divide(new BigDecimal(4)));
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, new BigDecimal(4), null),
        new BigDecimal(10).divide(new BigDecimal(4)));
  }
}
