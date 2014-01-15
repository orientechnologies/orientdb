/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public abstract class OAbstractMapCache<T extends Map<ORID, ?>> implements OCache {
  protected final OSharedResourceAdaptiveExternal lock    = new OSharedResourceAdaptiveExternal(
                                                              OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), 0,
                                                              true);
  protected final T                               cache;
  private final AtomicBoolean                     enabled = new AtomicBoolean(false);

  public OAbstractMapCache(T cache) {
    this.cache = cache;
  }

  @Override
  public void startup() {
    enable();
  }

  @Override
  public void shutdown() {
    disable();
  }

  @Override
  public boolean isEnabled() {
    return enabled.get();
  }

  @Override
  public boolean enable() {
    return enabled.compareAndSet(false, true);
  }

  @Override
  public boolean disable() {
    clear();
    return enabled.compareAndSet(true, false);
  }

  @Override
  public void clear() {
    if (!isEnabled())
      return;

    lock.acquireExclusiveLock();
    try {
      cache.clear();
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public int size() {
    lock.acquireSharedLock();
    try {
      return cache.size();
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public Collection<ORID> keys() {
    lock.acquireExclusiveLock();
    try {
      return new ArrayList<ORID>(cache.keySet());
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void lock(final ORID id) {
    lock.acquireExclusiveLock();
  }

  @Override
  public void unlock(final ORID id) {
    lock.releaseExclusiveLock();
  }
}
