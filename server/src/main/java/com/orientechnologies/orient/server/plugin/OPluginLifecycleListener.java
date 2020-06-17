/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.server.plugin;

import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

/**
 * Interface for monitoring plugin events.
 *
 * @author SDIPro
 */
public interface OPluginLifecycleListener {
  void onBeforeConfig(final OServerPlugin plugin, final OServerParameterConfiguration[] cfg);

  void onAfterConfig(final OServerPlugin plugin, final OServerParameterConfiguration[] cfg);

  void onBeforeStartup(final OServerPlugin plugin);

  void onAfterStartup(final OServerPlugin plugin);

  void onBeforeShutdown(final OServerPlugin plugin);

  void onAfterShutdown(final OServerPlugin plugin);
}
