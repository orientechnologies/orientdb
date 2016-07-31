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
package com.orientechnologies.orient;

import com.orientechnologies.orient.graph.stresstest.OGraphInsertWorkload;
import com.orientechnologies.orient.stresstest.OStressTester;
import com.orientechnologies.orient.stresstest.OStressTesterCommandLineParser;
import org.junit.Test;

/**
 * Test for Graph insert workload.
 *
 * @author Luca Garulli
 */
public class TestGraphInsertWorkload {

  @Test
  public void testParsing() throws Exception {

    new OGraphInsertWorkload().parseParameters("V10000");
    new OGraphInsertWorkload().parseParameters("V10000F10");
  }

  @Test
  public void testExecution() throws Exception {
    final OStressTester stressTester = OStressTesterCommandLineParser
        .getStressTester(new String[] { "-m", "plocal", "-c", "8", "-tx", "3", "-w", "GINSERT:V1000F10" });
    stressTester.execute();
  }
}
