/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class OSequenceLibraryProxy extends OSequenceLibraryAbstract {
  private static final int replicationProtocolVersion =
      OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.getValue();

  public OSequenceLibraryProxy(
      final OSequenceLibraryImpl iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public Set<String> getSequenceNames() {
    return delegate.getSequenceNames(database);
  }

  @Override
  public int getSequenceCount() {
    return delegate.getSequenceCount(database);
  }

  @Override
  public OSequence getSequence(String iName) {
    return delegate.getSequence(database, iName);
  }

  @Override
  public OSequence createSequence(
      String iName, SEQUENCE_TYPE sequenceType, OSequence.CreateParams params)
      throws ODatabaseException {
    boolean shouldGoOverDistributted =
        database.isDistributed() && (replicationProtocolVersion == 2);
    return createSequence(iName, sequenceType, params, shouldGoOverDistributted);
  }

  @Override
  OSequence createSequence(
      String iName,
      SEQUENCE_TYPE sequenceType,
      OSequence.CreateParams params,
      boolean executeViaDistributed)
      throws ODatabaseException {
    if (executeViaDistributed) {
      OSequenceAction action =
          new OSequenceAction(OSequenceAction.CREATE, iName, params, sequenceType);
      try {
        String sequenceName = database.sendSequenceAction(action);
        return delegate.getSequence(database, sequenceName);
      } catch (InterruptedException | ExecutionException exc) {
        OLogManager.instance().error(this, exc.getMessage(), exc, (Object[]) null);
        throw new ODatabaseException(exc.getMessage());
      }
    } else {
      return delegate.createSequence(database, iName, sequenceType, params);
    }
  }

  @Override
  @Deprecated
  public void dropSequence(String iName) throws ODatabaseException {
    boolean shouldGoOverDistributted =
        database.isDistributed() && (replicationProtocolVersion == 2);
    dropSequence(iName, shouldGoOverDistributted);
  }

  @Override
  void dropSequence(String iName, boolean executeViaDistributed) throws ODatabaseException {
    if (executeViaDistributed) {
      OSequenceAction action = new OSequenceAction(OSequenceAction.REMOVE, iName, null, null);
      try {
        database.sendSequenceAction(action);
      } catch (InterruptedException | ExecutionException exc) {
        OLogManager.instance().error(this, exc.getMessage(), exc, (Object[]) null);
        throw new ODatabaseException(exc.getMessage());
      }
    } else {
      delegate.dropSequence(database, iName);
    }
  }

  @Override
  public void create() {
    delegate.create(database);
  }

  @Override
  public void load() {
    delegate.load(database);
  }

  @Override
  public void close() {
    delegate.close();
  }

  public OSequenceLibraryImpl getDelegate() {
    return delegate;
  }
}
