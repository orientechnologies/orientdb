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

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;

/**
 * Thread Local to store execution setting.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OExecutionThreadLocal extends ThreadLocal<OExecutionThreadLocal.OExecutionThreadData> {
  public class OExecutionThreadData {
    public volatile OAsyncReplicationOk onAsyncReplicationOk;
    public volatile OAsyncReplicationError onAsyncReplicationError;
  }

  @Override
  protected OExecutionThreadData initialValue() {
    return new OExecutionThreadData();
  }

  public static volatile OExecutionThreadLocal INSTANCE = new OExecutionThreadLocal();

  public static boolean isInterruptCurrentOperation() {
    final Thread t = Thread.currentThread();
    if (t instanceof OSoftThread) return ((OSoftThread) t).isShutdownFlag();
    return false;
  }

  public void setInterruptCurrentOperation(final Thread t) {
    if (t instanceof OSoftThread) ((OSoftThread) t).softShutdown();
  }

  public static void setInterruptCurrentOperation() {
    final Thread t = Thread.currentThread();
    if (t instanceof OSoftThread) ((OSoftThread) t).softShutdown();
  }

  static {
    final Orient inst = Orient.instance();
    inst.registerListener(
        new OOrientListenerAbstract() {
          @Override
          public void onStartup() {
            if (INSTANCE == null) INSTANCE = new OExecutionThreadLocal();
          }

          @Override
          public void onShutdown() {
            INSTANCE = null;
          }
        });
  }
}
