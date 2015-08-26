/*
 *
 * Copyright 2013 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.core.metadata;

import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OIdentity;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.schedule.OSchedulerListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author luca.molino
 * 
 */
public interface OMetadata {
  Set<String> SYSTEM_CLUSTER = new HashSet<String>(Arrays.asList(new String[] { OUser.CLASS_NAME.toLowerCase(),
      ORole.CLASS_NAME.toLowerCase(), OIdentity.CLASS_NAME.toLowerCase(), "ORIDs".toLowerCase(),
      OSecurity.RESTRICTED_CLASSNAME.toLowerCase(), "OFunction".toLowerCase(), "OTriggered".toLowerCase(),
      "OSchedule".toLowerCase() }));

  void load();

  void create() throws IOException;

  OSchema getSchema();

  OSecurity getSecurity();

  OIndexManagerProxy getIndexManager();

  int getSchemaClusterId();

  /**
   * Reloads the internal objects.
   */
  void reload();

  /**
   * Closes internal objects
   */
  void close();

  OFunctionLibrary getFunctionLibrary();

  OSchedulerListener getSchedulerListener();
}
