/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.cache;

import java.util.Collection;
import java.util.Collections;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * @author Andrey Lomakin
 * @since 05.07.13
 */
public class OEmptyCache implements OCache {
  @Override
  public void startup() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public boolean enable() {
    return false;
  }

  @Override
  public boolean disable() {
    return false;
  }

  @Override
  public ORecordInternal<?> get(ORID id) {
    return null;
  }

  @Override
  public ORecordInternal<?> put(ORecordInternal<?> record) {
    return null;
  }

  @Override
  public ORecordInternal<?> remove(ORID id) {
    return null;
  }

  @Override
  public void clear() {
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int limit() {
    return 0;
  }

  @Override
  public Collection<ORID> keys() {
    return Collections.emptyList();
  }

  @Override
  public void lock(ORID id) {
  }

  @Override
  public void unlock(ORID id) {
  }
}
