/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.schedule.OScheduler;
import java.io.IOException;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public interface OMetadata {

  @Deprecated
  void load();

  @Deprecated
  void create() throws IOException;

  OSchema getSchema();

  OSecurity getSecurity();

  /** @deprecated Manual indexes are deprecated and will be removed */
  @Deprecated
  OIndexManager getIndexManager();

  @Deprecated
  int getSchemaClusterId();

  /** Reloads the internal objects. */
  void reload();

  /** Closes internal objects */
  @Deprecated
  void close();

  OFunctionLibrary getFunctionLibrary();

  OSequenceLibrary getSequenceLibrary();

  OScheduler getScheduler();
}
