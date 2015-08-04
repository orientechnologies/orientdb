/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
package com.orientechnologies.orient.server.plugin.livequery;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.sql.OLiveCommandExecutorSQLFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by Luigi Dell'Aquila
 */
public class OLiveQueryPlugin extends OServerPluginAbstract implements ODatabaseLifecycleListener {

  private boolean enabled = false;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    super.config(iServer, iParams);
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.value))
          enabled = true;
      }
    }
  }

  @Override
  public String getName() {
    return "LiveQueryPlugin";
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LATE;
  }

  @Override
  public void startup() {
    super.startup();
    if (this.enabled) {
      OLiveCommandExecutorSQLFactory.init();
      Orient.instance().addDbLifecycleListener(this);
    }
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {

  }

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {
    if (this.enabled) {
      if (iDatabase instanceof ODatabaseDocumentTx) {
        iDatabase.registerHook(new OLiveQueryHook((ODatabaseDocumentTx) iDatabase), ORecordHook.HOOK_POSITION.LAST);
      }
    }
  }

  @Override
  public void onClose(ODatabaseInternal iDatabase) {
    if (this.enabled) {
      if (iDatabase.getHooks() != null) {
        OLiveQueryHook toUnregister = null;
        for (Object hook : iDatabase.getHooks().keySet()) {
          if (hook instanceof OLiveQueryHook) {
            toUnregister = (OLiveQueryHook) hook;
            break;
          }
        }
        if (toUnregister != null) {
          iDatabase.unregisterHook(toUnregister);
        }
      }
    }
  }

  @Override
  public void onDrop(ODatabaseInternal iDatabase) {
  }

  @Override
  public void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {

  }
}
