/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.hazelcast.sharding;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

/**
 * Abstract class for query execution in autosharded environment
 * 
 * @author edegtyarenko
 * @since 25.10.12 8:08
 */
public abstract class OQueryExecutor {

  protected final OCommandRequestText iCommand;
  protected final OStorageEmbedded    wrapped;

  protected OQueryExecutor(OCommandRequestText iCommand, OStorageEmbedded wrapped) {
    this.iCommand = iCommand;
    this.wrapped = wrapped;
  }

  /**
   * Determine way of command execution
   * 
   * @return query result
   */
  public abstract Object execute();
}
