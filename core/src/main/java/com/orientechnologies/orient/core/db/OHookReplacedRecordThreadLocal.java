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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.ORecord;

/** Uses Thread Local to store information used by hooks. */
public class OHookReplacedRecordThreadLocal extends ThreadLocal<ORecord> {

  public static volatile OHookReplacedRecordThreadLocal INSTANCE =
      new OHookReplacedRecordThreadLocal();

  static {
    Orient.instance()
        .registerListener(
            new OOrientListenerAbstract() {
              @Override
              public void onStartup() {
                if (INSTANCE == null) INSTANCE = new OHookReplacedRecordThreadLocal();
              }

              @Override
              public void onShutdown() {
                INSTANCE = null;
              }
            });
  }

  public ORecord getIfDefined() {
    return super.get();
  }

  public boolean isDefined() {
    return super.get() != null;
  }
}
