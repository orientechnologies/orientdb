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
 */

package com.orientechnologies;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.Orient;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

/**
 * Listens for the JUnit test run finishing and runs the direct memory leaks detector, if no tests failed. If leak detector finds
 * some leaks, it triggers {@link AssertionError} and the build is marked as failed. Java assertions (-ea) must be active for this
 * to work.
 *
 * @author Sergey Sitnikov
 */
public class OJUnitTestLeaksListener extends RunListener {

  @Override
  public void testRunFinished(Result result) throws Exception {
    super.testRunFinished(result);

    Orient.instance().closeAllStorages();

    if (result.wasSuccessful()) {
      System.out.println("Checking for direct memory leaks...");
      OByteBufferPool.instance().verifyState();
    }
  }

}
