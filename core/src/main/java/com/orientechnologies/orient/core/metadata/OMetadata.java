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

import java.io.IOException;

import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.schedule.OSchedulerListener;

/**
 * @author luca.molino
 * 
 */
public interface OMetadata {

  public void load();

  public void create() throws IOException;

  public OSchema getSchema();

  public OSecurity getSecurity();

  public OIndexManagerProxy getIndexManager();

  public int getSchemaClusterId();

  /**
   * Reloads the internal objects.
   */
  public void reload();

  /**
   * Closes internal objects
   */
  public void close();

  public OFunctionLibrary getFunctionLibrary();

  public OSchedulerListener getSchedulerListener();
}
