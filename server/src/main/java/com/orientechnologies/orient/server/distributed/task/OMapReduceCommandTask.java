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
package com.orientechnologies.orient.server.distributed.task;

/**
 * Distributed map and reduce task that collect and merge the result.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OMapReduceCommandTask extends OSQLCommandTask {
  private static final long serialVersionUID = 1L;

  public OMapReduceCommandTask() {
  }

  public OMapReduceCommandTask(final String iCommand) {
    super(iCommand);
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.MERGE;
  }

  public boolean isWriteOperation() {
    return false;
  }

  @Override
  public OMapReduceCommandTask copy() {
    final OMapReduceCommandTask copy = (OMapReduceCommandTask) super.copy(new OMapReduceCommandTask());
    return copy;
  }

  public OMapReduceCommandTask copy(final OMapReduceCommandTask iCopy) {
    super.copy(iCopy);
    return iCopy;
  }

  @Override
  public String getName() {
    return "map_reduce_command";
  }
}
