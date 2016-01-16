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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */
public class OSchedulerListenerImpl implements OSchedulerListener {
  private static Map<String, OScheduler> schedulers = new ConcurrentHashMap<String, OScheduler>();

  public OSchedulerListenerImpl() {
  }

  public void addScheduler(OScheduler scheduler) {
    if (!schedulers.containsKey(scheduler.getSchduleName())) {
      schedulers.put(scheduler.getSchduleName(), scheduler);
    }
  }

  public void removeScheduler(OScheduler scheduler) {
    if (scheduler.isRunning())
      throw new OException("Cannot delete scheduler " + scheduler.getSchduleName() + " due to it is still running");
    schedulers.remove(scheduler.getSchduleName());
  }

  public Map<String, OScheduler> getSchedulers() {
    return schedulers;
  }

  public OScheduler getScheduler(String name) {
    return schedulers.get(name);
  }

  // loaded when open database
  public void load() {
    schedulers.clear();
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db.getMetadata().getSchema().existsClass(OScheduler.CLASSNAME)) {
      List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from " + OScheduler.CLASSNAME + " order by name"));
      for (ODocument d : result) {
        d.reload();
        this.addScheduler(new OScheduler(d));
      }
    }
  }

  public void close() {
    schedulers.clear();
  }

  public void create() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db.getMetadata().getSchema().existsClass(OScheduler.CLASSNAME))
      return;
    final OClassImpl f = (OClassImpl) db.getMetadata().getSchema().createClass(OScheduler.CLASSNAME);
    f.createProperty(OScheduler.PROP_NAME, OType.STRING, (OType) null, false).setMandatory(true).setNotNull(true);
    f.createProperty(OScheduler.PROP_RULE, OType.STRING, (OType) null, false).setMandatory(true).setNotNull(true);
    f.createProperty(OScheduler.PROP_ARGUMENTS, OType.EMBEDDEDMAP, (OType) null, false);
    f.createProperty(OScheduler.PROP_STATUS, OType.STRING, (OType) null, false);
    f.createProperty(OScheduler.PROP_FUNC, OType.LINK, db.getMetadata().getSchema().getClass(OFunction.CLASS_NAME), false)
        .setMandatory(true).setNotNull(true);
    f.createProperty(OScheduler.PROP_STARTTIME, OType.DATETIME, (OType) null, false);
    f.createProperty(OScheduler.PROP_STARTED, OType.BOOLEAN, (OType) null, false);
  }
}
