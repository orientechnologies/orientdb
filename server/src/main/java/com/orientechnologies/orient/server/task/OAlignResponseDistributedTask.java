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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;

/**
 * Distributed align response task to communicate the result of alignment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OAlignResponseDistributedTask extends OAbstractDistributedTask<Integer> {
  private static final long serialVersionUID = 1L;

  protected int             aligned;

  public OAlignResponseDistributedTask() {
  }

  public OAlignResponseDistributedTask(final String nodeSource, final String iDbName, final EXECUTION_MODE iMode, final int iAligned) {
    super(nodeSource, iDbName, iMode);
    aligned = iAligned;
  }

  @Override
  public Integer call() throws Exception {
    final ODistributedServerManager dManager = getDistributedServerManager();

    if (aligned == -1) {
      // ALIGNMENT POSTPONED
      OLogManager.instance().info(this, "DISTRIBUTED <-[%s/%s] alignment postponed", nodeSource, databaseName);

      dManager.postponeAlignment(nodeSource, databaseName);

    } else {
      // ALIGNMENT DONE
      OLogManager.instance().info(this, "DISTRIBUTED <-[%s/%s] alignment ended: %d operation(s)", nodeSource, databaseName, aligned);

      dManager.endAlignment(nodeSource, databaseName);
    }
    return null;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeInt(aligned);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    aligned = in.readInt();
  }

  @Override
  public String getName() {
    return "align_response";
  }
}
