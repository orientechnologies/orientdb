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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;

/**
 * Distributed align response task to communicate the result of alignment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OAlignResponseTask extends OAbstractRemoteTask<Integer> {
  private static final long serialVersionUID = 1L;

  protected int             aligned;

  public OAlignResponseTask() {
  }

  public OAlignResponseTask(final OServer iServer, final ODistributedServerManager iDistributedSrvMgr,
      final String iDbName, final EXECUTION_MODE iMode, final int iAligned) {
    super(iServer, iDistributedSrvMgr, iDbName, iMode);
    aligned = iAligned;
  }

  @Override
  public Integer call() throws Exception {
    final ODistributedServerManager dManager = getDistributedServerManager();

    if (aligned == -1) {
      // ALIGNMENT POSTPONED
      ODistributedServerLog.info(this, getDistributedServerManager().getLocalNodeId(), getNodeSource(), DIRECTION.IN,
          "alignment postponed for db '%s'", databaseName);

      dManager.postponeAlignment(getNodeSource(), databaseName);

    } else {
      // ALIGNMENT DONE
      ODistributedServerLog.info(this, getDistributedServerManager().getLocalNodeId(), getNodeSource(), DIRECTION.IN,
          "alignment ended against db '%s': %d operation(s)", databaseName, aligned);

      dManager.endAlignment(getNodeSource(), databaseName);
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
