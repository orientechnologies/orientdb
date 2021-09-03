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
import java.util.concurrent.Callable;

/**
 * Thread local to know when the request comes from distributed requester avoiding loops.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OScenarioThreadLocal extends ThreadLocal<OScenarioThreadLocal.RunContext> {
  public static volatile OScenarioThreadLocal INSTANCE = new OScenarioThreadLocal();

  public enum RUN_MODE {
    DEFAULT,
    RUNNING_DISTRIBUTED
  }

  public static class RunContext {
    public RUN_MODE runMode = RUN_MODE.DEFAULT;
  }

  static {
    Orient.instance()
        .registerListener(
            new OOrientListenerAbstract() {
              @Override
              public void onStartup() {
                if (INSTANCE == null) INSTANCE = new OScenarioThreadLocal();
              }

              @Override
              public void onShutdown() {
                INSTANCE = null;
              }
            });
  }

  public OScenarioThreadLocal() {
    setRunMode(RUN_MODE.DEFAULT);
  }

  public static <T> Object executeAsDefault(final Callable<T> iCallback) {
    final OScenarioThreadLocal.RUN_MODE currentDistributedMode =
        OScenarioThreadLocal.INSTANCE.getRunMode();
    if (currentDistributedMode == OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED)
      // ASSURE SCHEMA CHANGES ARE NEVER PROPAGATED ON CLUSTER
      OScenarioThreadLocal.INSTANCE.setRunMode(RUN_MODE.DEFAULT);

    try {
      return (T) iCallback.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (currentDistributedMode == OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED)
        // RESTORE PREVIOUS MODE
        OScenarioThreadLocal.INSTANCE.setRunMode(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);
    }
  }

  public static Object executeAsDistributed(final Callable<? extends Object> iCallback) {
    final OScenarioThreadLocal.RUN_MODE currentDistributedMode =
        OScenarioThreadLocal.INSTANCE.getRunMode();
    if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED)
      // ASSURE SCHEMA CHANGES ARE NEVER PROPAGATED ON CLUSTER
      OScenarioThreadLocal.INSTANCE.setRunMode(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);

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

  public void setRunMode(final RUN_MODE value) {
    final RunContext context = get();
    context.runMode = value;
  }

  public RUN_MODE getRunMode() {
    return get().runMode;
  }

  public boolean isRunModeDistributed() {
    return get().runMode == RUN_MODE.RUNNING_DISTRIBUTED;
  }

  @Override
  protected RunContext initialValue() {
    return new RunContext();
  }
}
