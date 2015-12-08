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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;

import java.util.concurrent.Callable;

/**
 * Thread local to know when the request comes from distributed requester avoiding loops.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OScenarioThreadLocal extends ThreadLocal<OScenarioThreadLocal.Scenario> {
  public static volatile OScenarioThreadLocal INSTANCE = new OScenarioThreadLocal();

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (INSTANCE == null)
          INSTANCE = new OScenarioThreadLocal();
      }

      @Override
      public void onShutdown() {
        INSTANCE = null;
      }
    });
  }

  public class Scenario {
    public RUN_MODE runMode             = RUN_MODE.DEFAULT;
    public boolean  replicationSyncMode = true;
  }

  public enum RUN_MODE {
    DEFAULT, RUNNING_DISTRIBUTED
  }

  public OScenarioThreadLocal() {
    setRunMode(RUN_MODE.DEFAULT);
  }

  public static Object executeAsDistributed(final Callable<Object> iCallback) {
    final RUN_MODE currentDistributedMode = INSTANCE.getRunMode();
    if (currentDistributedMode != RUN_MODE.RUNNING_DISTRIBUTED)
      // ASSURE SCHEMA CHANGES ARE NEVER PROPAGATED ON CLUSTER
      INSTANCE.setRunMode(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);

    try {
      return iCallback.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED)
        // RESTORE PREVIOUS MODE
        OScenarioThreadLocal.INSTANCE.setRunMode(OScenarioThreadLocal.RUN_MODE.DEFAULT);
    }
  }

  public static Object executeAsDefault(final Callable<Object> iCallback) {
    final RUN_MODE currentDistributedMode = INSTANCE.getRunMode();
    if (currentDistributedMode == RUN_MODE.RUNNING_DISTRIBUTED)
      // ASSURE SCHEMA CHANGES ARE NEVER PROPAGATED ON CLUSTER
      INSTANCE.setRunMode(RUN_MODE.DEFAULT);

    try {
      return iCallback.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (currentDistributedMode == RUN_MODE.RUNNING_DISTRIBUTED)
        // RESTORE PREVIOUS MODE
        INSTANCE.setRunMode(RUN_MODE.RUNNING_DISTRIBUTED);
    }
  }

  @Override
  public void set(final Scenario value) {
    super.set(value);
  }

  @Override
  protected Scenario initialValue() {
    return new Scenario();
  }

  public RUN_MODE getRunMode() {
    return get().runMode;
  }

  public void setRunMode(final RUN_MODE iRunMode) {
    get().runMode = iRunMode;
  }

  public boolean isReplicationSyncMode() {
    return get().replicationSyncMode;
  }

  public void setReplicationSyncMode(final boolean iReplicationSyncMode) {
    get().replicationSyncMode = iReplicationSyncMode;
  }
}
