/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/**
 * @author edegtyarenko
 * @since 11.10.12 14:40
 */
public class SQLFunctionDifferenceTest {

  @Test
  public void testExecute() {
    final OSQLFunctionDifference function = new OSQLFunctionDifference();

    List<List<Object>> incomes =
        Arrays.asList(
            Arrays.<Object>asList(1, 2, 3, 4, 5, 1),
            Arrays.<Object>asList(3, 5, 6, 7, 0, 1, 3, 3, 6),
            Arrays.<Object>asList(2, 2, 8, 9));

    Set<Object> expectedResult = new HashSet<Object>(Arrays.<Object>asList(4));

    Set<Object> actualResult =
        (Set<Object>)
            function.execute(null, null, null, incomes.toArray(), new OBasicCommandContext());

    assertSetEquals(actualResult, expectedResult);

    incomes =
        Arrays.asList(
            Arrays.<Object>asList(1, 2, 3, 4, 5, 1),
            Arrays.<Object>asList(3, 5, 6, 7, 0, 1, 3, 3, 6));

    expectedResult = new HashSet<Object>(Arrays.<Object>asList(2, 4));

    actualResult =
        (Set<Object>)
            function.execute(null, null, null, incomes.toArray(), new OBasicCommandContext());
    assertSetEquals(actualResult, expectedResult);
  }

  private void assertSetEquals(Set<Object> actualResult, Set<Object> expectedResult) {
    assertEquals(actualResult.size(), expectedResult.size());
    for (Object o : actualResult) {
      assertTrue(expectedResult.contains(o));
    }
  }
}
