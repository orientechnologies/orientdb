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
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */
public class OSchedulerListenerProxy extends OProxedResource<OSchedulerListener> implements OSchedulerListener {
  public OSchedulerListenerProxy(final OSchedulerListener iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public void addScheduler(OScheduler scheduler) {
    delegate.addScheduler(scheduler);
  }

  @Override
  public void removeScheduler(OScheduler scheduler) {
    delegate.removeScheduler(scheduler);
  }

  @Override
  public Map<String, OScheduler> getSchedulers() {
    return delegate.getSchedulers();
  }

  @Override
  public OScheduler getScheduler(String name) {
    return delegate.getScheduler(name);
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
