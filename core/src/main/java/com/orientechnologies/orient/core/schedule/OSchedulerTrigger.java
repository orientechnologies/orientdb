/*
 * Copyright 2010-2012 henryzhao81-at-gmail.com
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

package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.schedule.OScheduler.STATUS;

/**
 * Keeps synchronized the scheduled events in memory.
 * 
 * @author Luca Garulli
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */

public class OSchedulerTrigger extends ODocumentHookAbstract {

  public OSchedulerTrigger(ODatabaseDocument database) {
    super(database);
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public RESULT onTrigger(TYPE iType, ORecord iRecord) {
    OImmutableClass clazz = null;
    if (iRecord instanceof ODocument)
      clazz = ODocumentInternal.getImmutableSchemaClass((ODocument) iRecord);
    if (clazz == null || !clazz.isScheduler())
      return RESULT.RECORD_NOT_CHANGED;
    return super.onTrigger(iType, iRecord);
  }

  @Override
  public RESULT onRecordBeforeCreate(final ODocument iDocument) {
    String name = iDocument.field(OScheduledEvent.PROP_NAME);
    final OScheduledEvent event = database.getMetadata().getScheduler().getEvent(name);
    if (event != null && event.getDocument() != iDocument) {
      throw new ODatabaseException("Scheduled event with name '" + name + "' already exists in database");
    }

    final boolean start = iDocument.field(OScheduledEvent.PROP_STARTED) == null ? false
        : ((Boolean) iDocument.field(OScheduledEvent.PROP_STARTED));
    if (start)
      iDocument.field(OScheduledEvent.PROP_STATUS, STATUS.WAITING.name());
    else
      iDocument.field(OScheduledEvent.PROP_STATUS, STATUS.STOPPED.name());
    iDocument.field(OScheduledEvent.PROP_STARTED, start);
    return RESULT.RECORD_CHANGED;
  }

  @Override
  public void onRecordAfterCreate(final ODocument iDocument) {
    database.getMetadata().getScheduler().scheduleEvent(new OScheduledEvent(iDocument));
  }

  @Override
  public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
    try {
      boolean isStart = iDocument.field(OScheduledEvent.PROP_STARTED) == null ? false
          : ((Boolean) iDocument.field(OScheduledEvent.PROP_STARTED));
      final String schedulerName = iDocument.field(OScheduledEvent.PROP_NAME);
      OScheduledEvent event = database.getMetadata().getScheduler().getEvent(schedulerName);

      if (isStart) {
        if (event == null) {
          event = new OScheduledEvent(iDocument);
          database.getMetadata().getScheduler().scheduleEvent(event);
        }
        String currentStatus = iDocument.field(OScheduledEvent.PROP_STATUS);
        if (currentStatus.equals(STATUS.STOPPED.name())) {
          iDocument.field(OScheduledEvent.PROP_STATUS, STATUS.WAITING.name());
        }
      } else {
        if (event != null) {
          iDocument.field(OScheduledEvent.PROP_STATUS, STATUS.STOPPED.name());
        }
      }
      if (event != null)
        event.fromStream(iDocument);
    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error when updating scheduler - " + ex.getMessage());
      return RESULT.RECORD_NOT_CHANGED;
    }
    return RESULT.RECORD_CHANGED;
  }

  @Override
  public RESULT onRecordBeforeDelete(final ODocument iDocument) {
    final String eventName = iDocument.field(OScheduledEvent.PROP_NAME);
    OScheduledEvent scheduler = database.getMetadata().getScheduler().getEvent(eventName);
    if (scheduler != null) {
      database.getMetadata().getScheduler().removeEvent(eventName);
    }
    return RESULT.RECORD_CHANGED;
  }
}
