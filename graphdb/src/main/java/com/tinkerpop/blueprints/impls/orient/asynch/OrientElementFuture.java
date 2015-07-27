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

package com.tinkerpop.blueprints.impls.orient.asynch;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.orient.OrientElement;

import java.util.Set;
import java.util.concurrent.Future;

public abstract class OrientElementFuture<T extends OrientElement> implements Element, OIdentifiable {
  protected final Future<T> future;

  public OrientElementFuture(final Future<T> future) {
    this.future = future;
  }

  @Override
  public <TY> TY getProperty(final String key) {
    try {
      return future.get().getProperty(key);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public Set<String> getPropertyKeys() {
    try {
      return future.get().getPropertyKeys();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public void setProperty(final String key, final Object value) {
    try {
      future.get().setProperty(key, value);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public <TY> TY removeProperty(final String key) {
    try {
      return future.get().removeProperty(key);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public void remove() {
    try {
      future.get().remove();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public Object getId() {
    try {
      return future.get().getId();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public ORID getIdentity() {
    return (ORID) getId();
  }

  @Override
  public ODocument getRecord() {
    try {
      return future.get().getRecord();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  public void reload() {
    try {
      future.get().reload();
    } catch (Exception e) {
      throw new OException("Cannot reload current element", e);
    }
  }

  @Override
  public int hashCode() {
    try {
      return future.get().hashCode();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public void lock(final boolean iExclusive) {
    try {
      future.get().lock(iExclusive);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public boolean isLocked() {
    try {
      return future.get().isLocked();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy() {
    try {
      return future.get().lockingStrategy();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public void unlock() {
    try {
      future.get().unlock();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public int compareTo(final OIdentifiable o) {
    try {
      return future.get().compareTo(o);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public int compare(final OIdentifiable o1, final OIdentifiable o2) {
    try {
      return future.get().compare(o1, o2);
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  public T get() {
    try {
      return (T) future.get();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }

  @Override
  public String toString() {
    try {
      return future.get().toString();
    } catch (Exception e) {
      throw new OException("Cannot retrieve the requested information", e);
    }
  }
}
