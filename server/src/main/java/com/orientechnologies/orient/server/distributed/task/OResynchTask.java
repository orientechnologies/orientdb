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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Execute a resynch task to align asynchronous nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OResynchTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;

  public OResynchTask() {
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    return Boolean.TRUE;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
  }

  public long getSynchronousTimeout(final int iSynchNodes) {
    return super.getSynchronousTimeout(iSynchNodes) * 3;
  }

  public long getTotalTimeout(final int iTotalNodes) {
    return super.getTotalTimeout(iTotalNodes) * 3;
  }

  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getName() {
    return "resynch";
  }
}
