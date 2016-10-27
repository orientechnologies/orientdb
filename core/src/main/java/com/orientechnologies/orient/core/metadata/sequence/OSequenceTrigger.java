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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Update the in-memory function library.
 *
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
public class OSequenceTrigger extends ODocumentHookAbstract {
  public OSequenceTrigger(ODatabaseDocument database) {
    super(database);
    setIncludeClasses(OSequence.CLASS_NAME);
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public void onRecordAfterCreate(final ODocument iDocument) {
    getSequenceLibrary().onSequenceCreated(iDocument);
  }

  private static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public void onRecordAfterUpdate(final ODocument iDocument) {
    getSequenceLibrary().onSequenceUpdated(iDocument);
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    getSequenceLibrary().onSequenceDropped(iDocument);
  }

  private OSequenceLibrary getSequenceLibrary() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    return db.getMetadata().getSequenceLibrary();
  }
}
