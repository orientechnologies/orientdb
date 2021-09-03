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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import java.util.concurrent.atomic.AtomicBoolean;

/** Abstract class to test when a node is down. */
public abstract class AbstractHARemoveNode extends AbstractServerClusterTxTest {
  protected AtomicBoolean lastNodeIsUp = new AtomicBoolean(true);

  @Override
  protected void onBeforeChecks() throws InterruptedException {
    // // WAIT UNTIL THE END
    waitFor(
        0,
        new OCallable<Boolean, ODatabaseDocument>() {
          @Override
          public Boolean call(ODatabaseDocument db) {
            final boolean ok = db.countClass("Person") >= expected;
            if (!ok)
              System.out.println(
                  "Server 0: FOUND "
                      + db.countClass("Person")
                      + " people instead of expected "
                      + expected);
            else
              System.out.println(
                  "Server 0: FOUND " + db.countClass("Person") + ", expected " + expected);
            return ok;
          }
        },
        10000);

    waitFor(
        2,
        new OCallable<Boolean, ODatabaseDocument>() {
          @Override
          public Boolean call(ODatabaseDocument db) {
            final long node2Expected =
                lastNodeIsUp.get()
                    ? expected
                    : expected - (count * writerCount * (serverInstance.size() - 1));

            final boolean ok = db.countClass("Person") >= node2Expected;
            if (!ok)
              System.out.println(
                  "Server 2: FOUND "
                      + db.countClass("Person")
                      + " people instead of expected "
                      + node2Expected);
            else
              System.out.println(
                  "Server 2: FOUND " + db.countClass("Person") + ", expected " + expected);
            return ok;
          }
        },
        10000);
  }
}
