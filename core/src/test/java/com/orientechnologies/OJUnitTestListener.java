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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import org.junit.Assert;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

/**
 *
 *
 * <ol>
 *   <li>Listens for JUnit test run started and prohibits logging of exceptions on storage level.
 *   <li>Listens for the JUnit test run finishing and runs the direct memory leaks detector, if no
 *       tests failed. If leak detector finds some leaks, it triggers {@link AssertionError} and the
 *       build is marked as failed. Java assertions (-ea) must be active for this to work.
 *   <li>Triggers {@link AssertionError} if {@link OLogManager} is shutdown before test is finished.
 *       We may miss some errors because {@link OLogManager} is shutdown
 * </ol>
 *
 * @author Sergey Sitnikov
 */
public class OJUnitTestListener extends RunListener {
  @Override
  public void testRunStarted(Description description) throws Exception {
    super.testRunStarted(description);

    OLogManager.instance().applyStorageFilter();
  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    super.testRunFinished(result);

    if (OLogManager.instance().isShutdown()) {
      final String msg = "LogManager was switched off before shutdown";

      System.err.println(msg);
      Assert.fail(msg);
    }

    if (result.wasSuccessful()) {
      System.out.println("Shutting down OrientDB engine and checking for direct memory leaks...");
      final Orient orient = Orient.instance();

      if (orient != null) {
        // state is verified during engine shutdown
        orient.shutdown();
      } else {
        OByteBufferPool.instance(null).checkMemoryLeaks();
      }
    }
  }
}
