/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.stresstest.OStressTester;
import com.orientechnologies.orient.stresstest.OStressTesterCommandLineParser;
import org.testng.annotations.Test;

/**
 * INtegration Tests for stress test workloads.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class StressTestWorkloadTest {

  @Test
  public void testCRUD() throws Exception {
    final OStressTester stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-m", "memory", "-db", "testCRUD", "-c", "8", "-tx", "3", "-w", "crud:C10R10U10D10"
            });
    stressTester.execute();
  }

  @Test
  public void testGraphInsert() throws Exception {
    final OStressTester stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-m",
              "memory",
              "-db",
              "testGraphInsert",
              "-c",
              "8",
              "-tx",
              "3",
              "-w",
              "GINSERT:V100F3"
            });
    stressTester.execute();
  }

  @Test
  public void testGraphInsertRandomStrategy() throws Exception {
    final OStressTester stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-m",
              "memory",
              "-db",
              "testGraphInsertRandomStrategy",
              "-c",
              "8",
              "-tx",
              "3",
              "-w",
              "GINSERT:V100F3Srandom"
            });
    stressTester.execute();
  }

  @Test
  public void testGraphInsertSuperNodeStrategy() throws Exception {
    final OStressTester stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-m",
              "memory",
              "-db",
              "testGraphInsertSuperNodeStrategy",
              "-c",
              "8",
              "-tx",
              "3",
              "-w",
              "GINSERT:V100F3Ssupernode"
            });
    stressTester.execute();
  }

  @Test
  public void testGraphShortestPath() throws Exception {
    final OStressTester stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-m",
              "memory",
              "-db",
              "testGraphShortestPath",
              "-c",
              "8",
              "-tx",
              "3",
              "-lb",
              "ROUND_ROBIN_CONNECT",
              "-w",
              "GINSERT:V100F3,GSP:L3"
            });
    stressTester.execute();
  }
}
