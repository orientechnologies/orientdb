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
import com.orientechnologies.orient.core.storage.cache.OSnowFlakeIdGen;
import com.orientechnologies.orient.core.storage.cache.OWriteCacheIdGen;

public abstract class OEngineAbstract implements OEngine {
  private static final OWriteCacheIdGen writeCacheIdGen = new OSnowFlakeIdGen();

  private boolean running = false;

  protected int generateStorageId() {
    return writeCacheIdGen.nextId();
  }

  protected String getMode(Map<String, String> iConfiguration) {
    String dbMode = null;
    if (iConfiguration != null)
      dbMode = iConfiguration.get("mode");

    if (dbMode == null)
      dbMode = "rw";
    return dbMode;
  }

  @Override
  public void startup() {
    this.running = true;
  }

  @Override
  public void shutdown() {
    this.running = false;
  }

  public void removeStorage(final OStorage iStorage) {
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}
