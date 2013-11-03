/*
 * Copyright 1999-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.listener;

import java.util.Collection;
import java.util.HashSet;

import com.orientechnologies.common.concur.lock.OLock;
import com.orientechnologies.common.concur.lock.ONoLock;

/**
 * Abstract class to manage listeners.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <L>
 *          Listener type
 */
public abstract class OListenerManger<L> {
  private final Collection<L> listeners;
  private final OLock         lock;

  public OListenerManger() {
    this(new HashSet<L>(8), new ONoLock());
  }

  public OListenerManger(final OLock iLock) {
    listeners = new HashSet<L>(8);
    lock = iLock;
  }

  public OListenerManger(final Collection<L> iListeners, final OLock iLock) {
    listeners = iListeners;
    lock = iLock;
  }

  public void registerListener(final L iListener) {
    if (iListener != null) {
      lock.lock();
      try {

        listeners.add(iListener);

      } finally {
        lock.unlock();
      }
    }
  }

  public void unregisterListener(final L iListener) {
    if (iListener != null) {
      lock.lock();
      try {

        listeners.remove(iListener);

      } finally {
        lock.unlock();
      }
    }
  }

  public void resetListeners() {
    lock.lock();
    try {

      listeners.clear();

    } finally {
      lock.unlock();
    }
  }

  public Iterable<L> browseListeners() {
    return listeners;
  }

  @SuppressWarnings("unchecked")
  public Iterable<L> getListenersCopy() {
    lock.lock();
    try {

      return (Iterable<L>) new HashSet<Object>(listeners);

    } finally {
      lock.unlock();
    }
  }

  public OLock getLock() {
    return lock;
  }
}
