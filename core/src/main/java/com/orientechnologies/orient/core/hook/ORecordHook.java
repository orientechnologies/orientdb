/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.hook;

import com.orientechnologies.orient.core.record.ORecord;

/**
 * Hook interface to catch all events regarding records.
 * 
 * @author Luca Garulli
 * @see ORecordHookAbstract
 * 
 */
public interface ORecordHook {
  enum DISTRIBUTED_EXECUTION_MODE {
    TARGET_NODE, SOURCE_NODE, BOTH
  }

  enum HOOK_POSITION {
    FIRST, EARLY, REGULAR, LATE, LAST
  }

  enum TYPE {
    ANY, BEFORE_CREATE, BEFORE_READ, BEFORE_UPDATE, BEFORE_DELETE, AFTER_CREATE, AFTER_READ, AFTER_UPDATE, AFTER_DELETE,

    CREATE_FAILED, READ_FAILED, UPDATE_FAILED, DELETE_FAILED, CREATE_REPLICATED, READ_REPLICATED, UPDATE_REPLICATED,

    DELETE_REPLICATED, FINALIZE_UPDATE, FINALIZE_CREATION
  }

  enum RESULT {
    RECORD_NOT_CHANGED, RECORD_CHANGED, SKIP, SKIP_IO, RECORD_REPLACED
  }

  void onUnregister();

  RESULT onTrigger(TYPE iType, ORecord iRecord);

  DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode();
}
