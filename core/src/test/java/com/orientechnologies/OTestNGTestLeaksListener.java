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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;

import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ISuiteResult;

/**
 * Listens for the TestNG test run finishing and runs the direct memory leaks detector, if no tests failed. If leak detector finds
 * some leaks, it triggers {@link AssertionError} and the build is marked as failed. Java assertions (-ea) must be active for this
 * to work.
 *
 * @author Sergey Sitnikov
 */
public class OTestNGTestLeaksListener implements ISuiteListener {

  @Override
  public void onStart(ISuite suite) {
    // do nothing
  }

  @Override
  public void onFinish(ISuite suite) {

    if (!isFailed(suite)) {
      ODatabaseDocumentTx.closeAll();
      System.out.println("Checking for direct memory leaks...");
      OByteBufferPool.instance().verifyState();
    }
  }

  private static boolean isFailed(ISuite suite) {
    if (suite.getSuiteState().isFailed())
      return true;

    for (ISuiteResult result : suite.getResults().values())
      if (result.getTestContext().getFailedTests().size() != 0)
        return true;

    return false;
  }

}
