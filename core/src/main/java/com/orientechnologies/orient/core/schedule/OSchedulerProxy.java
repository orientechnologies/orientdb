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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OProxedResource;

import java.util.Map;

/**
 * Proxy implementation of the Scheduler.
 * 
 * @author Luca Garulli
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */
public class OSchedulerProxy extends OProxedResource<OScheduler> implements OScheduler {
  public OSchedulerProxy(final OScheduler iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public void scheduleEvent(final OScheduledEvent scheduler) {
    delegate.scheduleEvent(scheduler);
  }

  @Override
  public void removeEvent(final String eventName) {
    delegate.removeEvent(eventName);
  }

  @Override
  public Map<String, OScheduledEvent> getEvents() {
    return delegate.getEvents();
  }

  @Override
  public OScheduledEvent getEvent(final String name) {
    return delegate.getEvent(name);
  }

  @Override
  public void load() {
    delegate.load();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void create() {
    delegate.create();
  }
}
