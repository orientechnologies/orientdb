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
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.OStorageRemoteThreadLocal.OStorageRemoteSession;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;

public class OStorageRemoteThreadLocal extends ThreadLocal<OStorageRemoteSession> {
  public static volatile OStorageRemoteThreadLocal INSTANCE = new OStorageRemoteThreadLocal();

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (INSTANCE == null)
          INSTANCE = new OStorageRemoteThreadLocal();
      }

      @Override
      public void onShutdown() {
        INSTANCE = null;
      }
    });
  }

  public class OStorageRemoteSession {
    public boolean commandExecuting = false;
    public Integer sessionId        = -1;
    public String  serverURL        = null;
    public byte[]  token            = null;
    public String  connectionUserName;
    public String  connectionUserPassword;
  }

  @Override
  protected OStorageRemoteSession initialValue() {
    return new OStorageRemoteSession();
  }
}
