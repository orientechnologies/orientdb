/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.hook;

import com.orientechnologies.orient.core.record.ORecord;

/**
 * Hook abstract class that calls separate methods for each hook defined.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see ORecordHook
 */
public abstract class ORecordHookAbstract implements ORecordHook {

  /** Called on unregistration. */
  public void onUnregister() {}

  /**
   * It's called just before to create the new iRecord.
   *
   * @param iRecord The iRecord to create
   * @return True if the iRecord has been modified and a new marshalling is required, otherwise
   *     false
   */
  public RESULT onRecordBeforeCreate(final ORecord iRecord) {
    return RESULT.RECORD_NOT_CHANGED;
  }

  /**
   * It's called just after the iRecord is created.
   *
   * @param iRecord The iRecord just created
   */
  public void onRecordAfterCreate(final ORecord iRecord) {}

  public void onRecordCreateFailed(final ORecord iRecord) {}

  public void onRecordCreateReplicated(final ORecord iRecord) {}

  /**
   * It's called just before to read the iRecord.
   *
   * @param iRecord The iRecord to read
   * @return
   */
  public RESULT onRecordBeforeRead(final ORecord iRecord) {
    return RESULT.RECORD_NOT_CHANGED;
  }

  /**
   * It's called just after the iRecord is read.
   *
   * @param iRecord The iRecord just read
   */
  public void onRecordAfterRead(final ORecord iRecord) {}

  public void onRecordReadFailed(final ORecord iRecord) {}

  public void onRecordReadReplicated(final ORecord iRecord) {}

  /**
   * It's called just before to update the iRecord.
   *
   * @param iRecord The iRecord to update
   * @return True if the iRecord has been modified and a new marshalling is required, otherwise
   *     false
   */
  public RESULT onRecordBeforeUpdate(final ORecord iRecord) {
    return RESULT.RECORD_NOT_CHANGED;
  }

  /**
   * It's called just after the iRecord is updated.
   *
   * @param iRecord The iRecord just updated
   */
  public void onRecordAfterUpdate(final ORecord iRecord) {}

  public void onRecordUpdateFailed(final ORecord iRecord) {}

  public void onRecordUpdateReplicated(final ORecord iRecord) {}

  /**
   * It's called just before to delete the iRecord.
   *
   * @param iRecord The iRecord to delete
   * @return True if the iRecord has been modified and a new marshalling is required, otherwise
   *     false
   */
  public RESULT onRecordBeforeDelete(final ORecord iRecord) {
    return RESULT.RECORD_NOT_CHANGED;
  }

  /**
   * It's called just after the iRecord is deleted.
   *
   * @param iRecord The iRecord just deleted
   */
  public void onRecordAfterDelete(final ORecord iRecord) {}

  public void onRecordDeleteFailed(final ORecord iRecord) {}

  public void onRecordDeleteReplicated(final ORecord iRecord) {}

  public RESULT onRecordBeforeReplicaAdd(final ORecord record) {
    return RESULT.RECORD_NOT_CHANGED;
  }

  public void onRecordAfterReplicaAdd(final ORecord record) {}

  public void onRecordReplicaAddFailed(final ORecord record) {}

  public RESULT onRecordBeforeReplicaUpdate(final ORecord record) {
    return RESULT.RECORD_NOT_CHANGED;
  }

  public void onRecordAfterReplicaUpdate(final ORecord record) {}

  public void onRecordReplicaUpdateFailed(final ORecord record) {}

  public RESULT onRecordBeforeReplicaDelete(final ORecord record) {
    return RESULT.RECORD_NOT_CHANGED;
  }

  public void onRecordAfterReplicaDelete(final ORecord record) {}

  public void onRecordReplicaDeleteFailed(final ORecord record) {}

  public void onRecordFinalizeUpdate(final ORecord record) {}

  public void onRecordFinalizeCreation(final ORecord record) {}

  public void onRecordFinalizeDeletion(final ORecord record) {}

  public RESULT onTrigger(final TYPE iType, final ORecord record) {
    switch (iType) {
      case BEFORE_CREATE:
        return onRecordBeforeCreate(record);

      case AFTER_CREATE:
        onRecordAfterCreate(record);
        break;

      case CREATE_FAILED:
        onRecordCreateFailed(record);
        break;

      case CREATE_REPLICATED:
        onRecordCreateReplicated(record);
        break;

      case BEFORE_READ:
        return onRecordBeforeRead(record);

      case AFTER_READ:
        onRecordAfterRead(record);
        break;

      case READ_FAILED:
        onRecordReadFailed(record);
        break;

      case READ_REPLICATED:
        onRecordReadReplicated(record);
        break;

      case BEFORE_UPDATE:
        return onRecordBeforeUpdate(record);

      case AFTER_UPDATE:
        onRecordAfterUpdate(record);
        break;

      case UPDATE_FAILED:
        onRecordUpdateFailed(record);
        break;

      case UPDATE_REPLICATED:
        onRecordUpdateReplicated(record);
        break;

      case BEFORE_DELETE:
        return onRecordBeforeDelete(record);

      case AFTER_DELETE:
        onRecordAfterDelete(record);
        break;

      case DELETE_FAILED:
        onRecordDeleteFailed(record);
        break;

      case DELETE_REPLICATED:
        onRecordDeleteReplicated(record);
        break;

      case FINALIZE_CREATION:
        onRecordFinalizeCreation(record);
        break;

      case FINALIZE_UPDATE:
        onRecordFinalizeUpdate(record);
        break;

      case FINALIZE_DELETION:
        onRecordFinalizeDeletion(record);
        break;
    }
    return RESULT.RECORD_NOT_CHANGED;
  }
}
