/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.plugin;

import com.orientechnologies.orient.server.handler.OServerPlugin;

/**
 * Server plugin information
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OServerPluginInfo {
  private final String        name;
  private final OServerPlugin instance;
  private final long          loadedOn;

  public OServerPluginInfo(final String name, final OServerPlugin instance, final long loadedOn) {
    this.name = name;
    this.loadedOn = loadedOn;
    this.instance = instance;
  }

  public OServerPluginInfo(final String name, final OServerPlugin instance) {
    this.name = name;
    this.instance = instance;
    this.loadedOn = 0;
  }

  public boolean isDynamic() {
    return loadedOn > 0;
  }

  public String getName() {
    return name;
  }

  public OServerPlugin getInstance() {
    return instance;
  }

  public long getLoadedOn() {
    return loadedOn;
  }
}
