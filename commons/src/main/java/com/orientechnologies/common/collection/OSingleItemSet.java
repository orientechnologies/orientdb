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

package com.orientechnologies.common.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Set implementation with one item only.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OSingleItemSet<T> implements Set<T> {
  protected T value;

  public OSingleItemSet(T value) {
    this.value = value;
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean contains(Object o) {
    return value.equals(o);
  }

  @Override
  public Iterator<T> iterator() {
    return new OIterableObject<T>(value);
  }

  @Override
  public Object[] toArray() {
    return new Object[] { value };
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    throw new UnsupportedOperationException("toArray([])");
  }

  @Override
  public boolean add(T t) {
    throw new UnsupportedOperationException("add");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("containsAll");
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException("addAll");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("retainAll");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("removeAll");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("clear");
  }
}
