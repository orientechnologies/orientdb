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

package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHookConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * User: kasper fock Date: 09/11/12 Time: 22:35 Registers hooks defined the in xml configuration.
 *
 * <p>Hooks can be defined in xml as :
 *
 * <p><hooks> <hook class="HookClass"> <parameters> <parameter name="foo" value="bar" />
 * </parameters> </hook> </hooks> In case any parameters is defined the hook class should have a
 * method with following signature: public void config(OServer oServer,
 * OServerParameterConfiguration[] iParams)
 */
public class OConfigurableHooksManager implements ODatabaseLifecycleListener {

  private List<OServerHookConfiguration> configuredHooks;

  public OConfigurableHooksManager(final OServerConfiguration iCfg) {
    configuredHooks = iCfg.hooks;
    if (configuredHooks != null && !configuredHooks.isEmpty())
      Orient.instance().addDbLifecycleListener(this);
  }

  public void addHook(OServerHookConfiguration configuration) {
    if (this.configuredHooks == null) {
      configuredHooks = new ArrayList<>();
      Orient.instance().addDbLifecycleListener(this);
    }
    configuredHooks.add(configuration);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    onOpen(iDatabase);
  }

  public void onOpen(ODatabaseInternal iDatabase) {
    if (!((ODatabaseDocumentInternal) iDatabase).isRemote()) {
      final ODatabase<?> db = (ODatabase<?>) iDatabase;
      for (OServerHookConfiguration hook : configuredHooks) {
        try {
          final ORecordHook.HOOK_POSITION pos = ORecordHook.HOOK_POSITION.valueOf(hook.position);
          Class<?> klass = Class.forName(hook.clazz);
          final ORecordHook h;
          Constructor constructor = null;
          try {
            constructor = klass.getConstructor(ODatabaseDocument.class);
          } catch (NoSuchMethodException ex) {
            // Ignore
          }

          if (constructor != null) {
            h = (ORecordHook) constructor.newInstance(iDatabase);
          } else {
            h = (ORecordHook) klass.newInstance();
          }
          if (hook.parameters != null && hook.parameters.length > 0)
            try {
              final Method m =
                  h.getClass()
                      .getDeclaredMethod(
                          "config", new Class[] {OServerParameterConfiguration[].class});
              m.invoke(h, new Object[] {hook.parameters});
            } catch (Exception e) {
              OLogManager.instance()
                  .warn(
                      this,
                      "[configure] Failed to configure hook '%s'. Parameters specified but hook don support parameters. Should have a method config with parameters OServerParameterConfiguration[] ",
                      hook.clazz);
            }
          db.registerHook(h, pos);
        } catch (Exception e) {
          OLogManager.instance()
              .error(
                  this,
                  "[configure] Failed to configure hook '%s' due to the an error : ",
                  e,
                  hook.clazz,
                  e.getMessage());
        }
      }
    }
  }

  @Override
  public void onClose(ODatabaseInternal iDatabase) {}

  @Override
  public void onDrop(ODatabaseInternal iDatabase) {}

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {}

  public String getName() {
    return "HookRegisters";
  }
}
