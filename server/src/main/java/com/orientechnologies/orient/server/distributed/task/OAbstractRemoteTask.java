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

import java.io.Externalizable;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Base class for Tasks to be executed remotely.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OAbstractRemoteTask implements Externalizable {
  private static final long   serialVersionUID = 1L;
  protected transient boolean inheritedDatabase;
  protected transient String  nodeSource;

  public enum RESULT_STRATEGY {
    ANY, UNION
  }

  public enum QUORUM_TYPE {
    NONE, READ, WRITE, ALL
  }

  /**
   * Constructor used from unmarshalling.
   */
  public OAbstractRemoteTask() {
  }

  public abstract String getName();

  public abstract QUORUM_TYPE getQuorumType();

  public abstract Object execute(OServer iServer, ODistributedServerManager iManager, ODatabaseDocumentTx database)
      throws Exception;

  public long getTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  public long getSynchronousTimeout(final int iSynchNodes) {
    return getTimeout() * iSynchNodes;
  }

  public long getTotalTimeout(final int iTotalNodes) {
    return getTimeout() * iTotalNodes;
  }

  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.ANY;
  }

  @Override
  public String toString() {
    return getName();
  }

  public String getNodeSource() {
    return nodeSource;
  }

  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  public void undo() {
  }

  public boolean isRequireNodeOnline() {
    return true;
  }

  public boolean isRequiredOpenDatabase() {
    return true;
  }

  public boolean isIdempotent() {
    return false;
  }
}
