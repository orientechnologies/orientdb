/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.server.plugin.livequery;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by Luigi Dell'Aquila
 *
 * <p>Not needed anymore, keeping the class for backward compatibilty
 */
@Deprecated
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
        if (Boolean.parseBoolean(param.value)) enabled = true;
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
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {}

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {}

  @Override
  public void onClose(ODatabaseInternal iDatabase) {}

  @Override
  public void onDrop(ODatabaseInternal iDatabase) {}

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {}
}
