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
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scheduler default implementation.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */
public class OSchedulerImpl {
  private ConcurrentHashMap<String, OScheduledEvent> events =
      new ConcurrentHashMap<String, OScheduledEvent>();

  private final OrientDBInternal orientDB;

  public OSchedulerImpl(OrientDBInternal orientDB) {
    this.orientDB = orientDB;
  }

  public void scheduleEvent(final OScheduledEvent event) {
    if (event.getDocument().getIdentity().isNew())
      // FIST TIME: SAVE IT
      event.save();

    if (events.putIfAbsent(event.getName(), event) == null) {
      String database = event.getDocument().getDatabase().getName();
      event.schedule(database, "admin", orientDB);
    }
  }

  public OScheduledEvent removeEventInternal(final String eventName) {
    final OScheduledEvent event = events.remove(eventName);

    if (event != null) {
      event.interrupt();
    }
    return event;
  }

  public void removeEvent(final String eventName) {
    OLogManager.instance().debug(this, "Removing scheduled event '%s'...", eventName);

    final OScheduledEvent event = removeEventInternal(eventName);

    if (event != null) {

      try {
        event.getDocument().reload();
      } catch (ORecordNotFoundException ignore) {
        // ALREADY DELETED, JUST RETURN
        return;
      }

      // RECORD EXISTS: DELETE THE EVENT RECORD
      ODatabaseDocumentAbstract.executeWithRetries(
          new OCallable<Object, Integer>() {
            @Override
            public Object call(Integer iArgument) {
              OLogManager.instance()
                  .debug(
                      this,
                      "Deleting scheduled event '%s' rid=%s...",
                      event,
                      event.getDocument().getIdentity());
              event.getDocument().delete();
              return null;
            }
          },
          10,
          0,
          new ORecord[] {event.getDocument()});
    }
  }

  public void updateEvent(final OScheduledEvent event) {
    final OScheduledEvent oldEvent = events.remove(event.getName());
    if (oldEvent != null) oldEvent.interrupt();
    scheduleEvent(event);
    OLogManager.instance()
        .debug(
            this,
            "Updated scheduled event '%s' rid=%s...",
            event,
            event.getDocument().getIdentity());
  }

  public Map<String, OScheduledEvent> getEvents() {
    return events;
  }

  public OScheduledEvent getEvent(final String name) {
    return events.get(name);
  }

  public void load() {
    throw new UnsupportedOperationException();
  }

  public void load(ODatabaseDocumentInternal database) {

    if (database.getMetadata().getSchema().existsClass(OScheduledEvent.CLASS_NAME)) {
      final Iterable<ODocument> result = database.browseClass(OScheduledEvent.CLASS_NAME);
      for (ODocument d : result) {
        scheduleEvent(new OScheduledEvent(d));
      }
    }
  }

  public void close() {
    for (OScheduledEvent event : events.values()) {
      event.interrupt();
    }
    events.clear();
  }

  public void create(ODatabaseDocumentInternal database) {
    if (database.getMetadata().getImmutableSchemaSnapshot().existsClass(OScheduledEvent.CLASS_NAME))
      return;
    final OClass f = database.getMetadata().getSchema().createClass(OScheduledEvent.CLASS_NAME);
    f.createProperty(OScheduledEvent.PROP_NAME, OType.STRING, (OType) null, true)
        .setMandatory(true)
        .setNotNull(true);
    f.createProperty(OScheduledEvent.PROP_RULE, OType.STRING, (OType) null, true)
        .setMandatory(true)
        .setNotNull(true);
    f.createProperty(OScheduledEvent.PROP_ARGUMENTS, OType.EMBEDDEDMAP, (OType) null, true);
    f.createProperty(OScheduledEvent.PROP_STATUS, OType.STRING, (OType) null, true);
    f.createProperty(
            OScheduledEvent.PROP_FUNC,
            OType.LINK,
            database.getMetadata().getSchema().getClass(OFunction.CLASS_NAME),
            true)
        .setMandatory(true)
        .setNotNull(true);
    f.createProperty(OScheduledEvent.PROP_STARTTIME, OType.DATETIME, (OType) null, true);
  }

  public void initScheduleRecord(ODocument doc) {
    String name = doc.field(OScheduledEvent.PROP_NAME);
    final OScheduledEvent event = getEvent(name);
    if (event != null && event.getDocument() != doc) {
      throw new ODatabaseException(
          "Scheduled event with name '" + name + "' already exists in database");
    }
    doc.field(OScheduledEvent.PROP_STATUS, OScheduler.STATUS.STOPPED.name());
  }

  public void handleUpdateSchedule(ODocument doc) {
    try {
      final String schedulerName = doc.field(OScheduledEvent.PROP_NAME);
      OScheduledEvent event = getEvent(schedulerName);

      if (event != null) {
        // UPDATED EVENT
        final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(doc.getDirtyFields()));

        if (dirtyFields.contains(OScheduledEvent.PROP_NAME))
          throw new OValidationException("Scheduled event cannot change name");

        if (dirtyFields.contains(OScheduledEvent.PROP_RULE)) {
          // RULE CHANGED, STOP CURRENT EVENT AND RESCHEDULE IT
          updateEvent(new OScheduledEvent(doc));
        } else {
          doc.field(OScheduledEvent.PROP_STATUS, OScheduler.STATUS.STOPPED.name());
          event.fromStream(doc);
        }
      }

    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error on updating scheduled event", ex);
    }
  }
}
