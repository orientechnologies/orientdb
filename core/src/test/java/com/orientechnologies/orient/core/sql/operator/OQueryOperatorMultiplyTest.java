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

import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMultiply;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;

/**
 * Created by luigidellaquila on 29/03/16.
 */

public class OQueryOperatorMultiplyTest {
  @Test
  public void test() {
    OQueryOperatorMultiply operator = new OQueryOperatorMultiply();
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, 10, null), 100);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10l, 10l, null), 100l);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10000000, 10000, null), 100000000000l);//upscale to long
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10.1, 10, null), 10.1 * 10);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, 10.1, null), 10 * 10.1);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10.1d, 10, null), 10.1d * 10);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, 10.1d, null), 10 * 10.1d);
    Assert.assertEquals(operator.evaluateRecord(null, null, null, new BigDecimal(10), 10, null),
        new BigDecimal(10).multiply(new BigDecimal(10)));
    Assert.assertEquals(operator.evaluateRecord(null, null, null, 10, new BigDecimal(10), null),
        new BigDecimal(10).multiply(new BigDecimal(10)));
  }
}
