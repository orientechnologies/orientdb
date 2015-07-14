/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.graph.GraphNoTxAbstractTest;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SQLGraphBatchTest extends GraphNoTxAbstractTest {
  @Test
  public void testTraverseContext() {
    StringBuilder script = new StringBuilder();

    script.append("LET v1 = CREATE VERTEX V SET name = 1;");
    script.append("LET v2 = CREATE VERTEX V SET name = 2; ");
    script.append("CREATE EDGE E FROM $v1 TO $v2; ");
    script.append("TRAVERSE * FROM $v1; ");

    Iterable<OrientElement> qResult = graph.command(new OCommandScript("sql", script.toString())).execute();

    final Iterator<OrientElement> it = qResult.iterator();

    assertTrue(it.hasNext());
    assertNotNull(it.next());
  }

  @BeforeClass
  public static void init() {
    init(SQLGraphBatchTest.class.getSimpleName());
  }
}
