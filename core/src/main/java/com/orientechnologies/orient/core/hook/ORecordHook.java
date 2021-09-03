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
 * Hook interface to catch all events regarding records.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) – initial contribution
 * @author Sergey Sitnikov – scoped hooks
 * @see ORecordHookAbstract
 */
public interface ORecordHook {
  enum DISTRIBUTED_EXECUTION_MODE {
    TARGET_NODE,
    SOURCE_NODE,
    BOTH
  }

  enum HOOK_POSITION {
    FIRST,
    EARLY,
    REGULAR,
    LATE,
    LAST
  }

  enum TYPE {
    ANY,

    BEFORE_CREATE,
    BEFORE_READ,
    BEFORE_UPDATE,
    BEFORE_DELETE,
    AFTER_CREATE,
    AFTER_READ,
    AFTER_UPDATE,
    AFTER_DELETE,

    CREATE_FAILED,
    READ_FAILED,
    UPDATE_FAILED,
    DELETE_FAILED,
    CREATE_REPLICATED,
    READ_REPLICATED,
    UPDATE_REPLICATED,

    DELETE_REPLICATED,
    FINALIZE_UPDATE,
    FINALIZE_CREATION,
    FINALIZE_DELETION
  }

  enum RESULT {
    RECORD_NOT_CHANGED,
    RECORD_CHANGED,
    SKIP,
    SKIP_IO,
    RECORD_REPLACED
  }

  /**
   * Defines available scopes for scoped hooks.
   *
   * <p>Basically, each scope defines some subset of {@link ORecordHook.TYPE}, this limits the set
   * of events the hook interested in and lowers the number of useless hook invocations.
   *
   * @see ORecordHook#getScopes()
   */
  enum SCOPE {
    /**
     * The create scope, includes: {@link ORecordHook.TYPE#BEFORE_CREATE}, {@link
     * ORecordHook.TYPE#AFTER_CREATE}, {@link ORecordHook.TYPE#FINALIZE_CREATION}, {@link
     * ORecordHook.TYPE#CREATE_REPLICATED} and {@link ORecordHook.TYPE#CREATE_FAILED}.
     */
    CREATE,

    /**
     * The read scope, includes: {@link ORecordHook.TYPE#BEFORE_READ}, {@link
     * ORecordHook.TYPE#AFTER_READ}, {@link ORecordHook.TYPE#READ_REPLICATED} and {@link
     * ORecordHook.TYPE#READ_FAILED}.
     */
    READ,

    /**
     * The update scope, includes: {@link ORecordHook.TYPE#BEFORE_UPDATE}, {@link
     * ORecordHook.TYPE#AFTER_UPDATE}, {@link ORecordHook.TYPE#FINALIZE_UPDATE}, {@link
     * ORecordHook.TYPE#UPDATE_REPLICATED} and {@link ORecordHook.TYPE#UPDATE_FAILED}.
     */
    UPDATE,

    /**
     * The delete scope, includes: {@link ORecordHook.TYPE#BEFORE_DELETE}, {@link
     * ORecordHook.TYPE#AFTER_DELETE}, {@link ORecordHook.TYPE#DELETE_REPLICATED}, {@link
     * ORecordHook.TYPE#DELETE_FAILED} and {@link ORecordHook.TYPE#FINALIZE_DELETION}.
     */
    DELETE;

    /**
     * Maps the {@link ORecordHook.TYPE} to {@link ORecordHook.SCOPE}.
     *
     * @param type the hook type to map.
     * @return the mapped scope.
     */
    public static SCOPE typeToScope(TYPE type) {
      switch (type) {
        case BEFORE_CREATE:
        case AFTER_CREATE:
        case CREATE_FAILED:
        case CREATE_REPLICATED:
        case FINALIZE_CREATION:
          return SCOPE.CREATE;

        case BEFORE_READ:
        case AFTER_READ:
        case READ_REPLICATED:
        case READ_FAILED:
          return SCOPE.READ;

        case BEFORE_UPDATE:
        case AFTER_UPDATE:
        case UPDATE_FAILED:
        case UPDATE_REPLICATED:
        case FINALIZE_UPDATE:
          return SCOPE.UPDATE;

        case BEFORE_DELETE:
        case AFTER_DELETE:
        case DELETE_FAILED:
        case DELETE_REPLICATED:
        case FINALIZE_DELETION:
          return SCOPE.DELETE;

        default:
          throw new IllegalStateException("Unexpected hook type.");
      }
    }
  }

  void onUnregister();

  RESULT onTrigger(TYPE iType, ORecord iRecord);

  DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode();

  /**
   * Returns the array of scopes this hook interested in. By default, all available scopes are
   * returned, implement/override this method to limit the scopes this hook may participate to lower
   * the number of useless invocations of this hook.
   *
   * <p>Limiting the hook to proper scopes may give huge performance boost, especially if the hook's
   * {@link #onTrigger(TYPE, ORecord)} dispatcher implementation is heavy. In extreme cases, you may
   * override the {@link #onTrigger(TYPE, ORecord)} to act directly on event's {@link
   * ORecordHook.TYPE} and exit early, scopes are just a more handy alternative to this.
   *
   * @return the scopes of this hook.
   * @see ORecordHook.SCOPE
   */
  default SCOPE[] getScopes() {
    return SCOPE.values();
  }
}
