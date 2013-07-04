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
package com.orientechnologies.orient.server.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;

/**
 * Groups multiples tasks to being replicated in one single call.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OMultipleDistributedTasks extends OAbstractDistributedTask<Object[]> {
  private static final long                 serialVersionUID = 1L;
  private List<OAbstractDistributedTask<?>> tasks            = new ArrayList<OAbstractDistributedTask<?>>();

  public OMultipleDistributedTasks() {
  }

  public OMultipleDistributedTasks(final String nodeSource, final String iDbName, final EXECUTION_MODE iMode) {
    super(nodeSource, iDbName, iMode);
  }

  @Override
  public Object[] call() throws Exception {
    OLogManager.instance().info(this, "DISTRIBUTED <-[%s/%s] executing group of %d command(s)", nodeSource, databaseName,
        tasks.size());

    final Object[] result = new Object[tasks.size()];

    for (int i = 0; i < tasks.size(); ++i) {
      final OAbstractDistributedTask<?> task = tasks.get(i);
      result[i] = task.call();
    }

    return result;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeInt(tasks.size());
    for (int i = 0; i < tasks.size(); ++i) {
      out.writeObject(tasks.get(i));
    }
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    final int taskSize = in.readInt();
    for (int i = 0; i < taskSize; ++i)
      tasks.add((OAbstractDistributedTask<?>) in.readObject());
  }

  @Override
  public String getName() {
    return "multiple_requests";
  }

  public int getTasks() {
    return tasks.size();
  }

  public void addTask(final OAbstractDistributedTask<?> operation) {
    tasks.add(operation);
  }

  public void clearTasks() {
    tasks.clear();
  }

  public OAbstractDistributedTask<?> getTask(final int i) {
    return tasks.get(i);
  }
}
