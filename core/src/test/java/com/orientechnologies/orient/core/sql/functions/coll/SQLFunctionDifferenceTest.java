/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.coll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.annotations.Test;

/**
 * @author edegtyarenko
 * @since 11.10.12 14:40
 */
@Test
public class SQLFunctionDifferenceTest {

  @Test
  public void testOperator() {
    final OSQLFunctionDifference differenceFunction = new OSQLFunctionDifference() {
      @Override
      protected boolean returnDistributedResult() {
        return false;
      }
    };

    final List<Object> income = Arrays.<Object> asList(1, 2, 3, 1, 4, 5, 2, 2, 1, 1);
    final Set<Object> expectedResult = new HashSet<Object>(Arrays.asList(3, 4, 5));

    for (Object i : income) {
      differenceFunction.execute(null, null, new Object[] { i }, null);
    }

    final Set<Object> actualResult = differenceFunction.getResult();

    assertSetEquals(actualResult, expectedResult);
  }

  @Test
  public void testOperatorMerge() {
    final OSQLFunctionDifference merger = new OSQLFunctionDifference();
    final List<OSQLFunctionDifference> differences = new ArrayList<OSQLFunctionDifference>(3);
    for (int i = 0; i < 3; i++) {
      differences.add(new OSQLFunctionDifference() {
        @Override
        protected boolean returnDistributedResult() {
          return true;
        }
      });
    }

    final List<List<Object>> incomes = Arrays.asList(Arrays.<Object> asList(1, 2, 3, 4, 5, 1),
        Arrays.<Object> asList(3, 5, 6, 7, 0, 1, 3, 3, 6), Arrays.<Object> asList(2, 2, 8, 9));

    final Set<Object> expectedResult = new HashSet<Object>(Arrays.<Object> asList(4, 7, 8, 9, 0));

    for (int j = 0; j < 3; j++) {
      for (Object i : incomes.get(j)) {
        differences.get(j).execute(null, null, new Object[] { i }, null);
      }
    }

    final Set<Object> actualResult = (Set<Object>) merger.mergeDistributedResult(Arrays.asList((Object) differences.get(0)
        .getResult(), differences.get(1).getResult(), differences.get(2).getResult()));

    assertSetEquals(actualResult, expectedResult);
  }

  private void assertSetEquals(Set<Object> actualResult, Set<Object> expectedResult) {
    assertEquals(actualResult.size(), expectedResult.size());
    for (Object o : actualResult) {
      assertTrue(expectedResult.contains(o));
    }
  }
}
