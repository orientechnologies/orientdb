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

/**
 * Distributed align response task to communicate the result of alignment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OAlignResponseTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;

  protected int             aligned;

  public OAlignResponseTask() {
  }

  public OAlignResponseTask(final int iAligned) {
    aligned = iAligned;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final String iDatabaseName) throws Exception {
    if (aligned == -1) {
      // ALIGNMENT POSTPONED
      ODistributedServerLog.info(this, iManager.getLocalNodeName(), null, DIRECTION.IN, "alignment postponed for db '%s'",
          iDatabaseName);

      iManager.postponeAlignment(iManager.getLocalNodeName(), iDatabaseName);

    } else {
      // ALIGNMENT DONE
      ODistributedServerLog.info(this, iManager.getLocalNodeName(), null, DIRECTION.IN,
          "alignment ended against db '%s': %d operation(s)", iDatabaseName, aligned);

      iManager.endAlignment(iManager.getLocalNodeName(), iDatabaseName, aligned);
    }
    return null;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeInt(aligned);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    aligned = in.readInt();
  }

  @Override
  public String getName() {
    return "align_response";
  }
}
