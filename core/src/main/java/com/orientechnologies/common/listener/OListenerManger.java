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
package com.orientechnologies.common.listener;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract class to manage listeners.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @param <L> Listener type
 */
public abstract class OListenerManger<L> {
  private final Collection<L> listeners;

  public OListenerManger(boolean concurrent) {
    if (concurrent) listeners = Collections.newSetFromMap(new ConcurrentHashMap<L, Boolean>());
    else listeners = new HashSet<L>();
  }

  public void registerListener(final L iListener) {
    if (iListener != null) {
      listeners.add(iListener);
    }
  }

  public void unregisterListener(final L iListener) {
    if (iListener != null) {
      listeners.remove(iListener);
    }
  }

  public void resetListeners() {
    listeners.clear();
  }

  public Iterable<L> browseListeners() {
    return listeners;
  }

  @SuppressWarnings("unchecked")
  public Iterable<L> getListenersCopy() {
    return (Iterable<L>) new HashSet<Object>(listeners);
  }
}
