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
package com.orientechnologies.orient.core.engine;

import java.util.Map;

import com.orientechnologies.orient.core.storage.OStorage;

public interface OEngine {
  String getName();

  OStorage createStorage(String iURL, Map<String, String> parameters);

  void removeStorage(OStorage iStorage);

  void shutdown();

  /**
   * Performs initialization of engine.
   * Initialization of engine in constructor is prohibited and all initialization steps should be done in
   * this method.
   */
  void startup();

  String getNameFromPath(String dbPath);

  /**
   * @return {@code true} if this engine has been started and not shutdown yet, {@code false} otherwise.
   */
  boolean isRunning();
}
