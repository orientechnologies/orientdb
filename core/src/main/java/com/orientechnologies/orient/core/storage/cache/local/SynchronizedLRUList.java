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

package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.local.HashLRUList;
import com.orientechnologies.orient.core.storage.cache.local.LRUList;

import java.util.Iterator;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class SynchronizedLRUList implements LRUList {
  private final LRUList underlying = new HashLRUList();

  @Override
  public synchronized OCacheEntry get(long fileId, long pageIndex) {
    return underlying.get(fileId, pageIndex);
  }

  @Override
  public synchronized OCacheEntry remove(long fileId, long pageIndex) {
    return underlying.remove(fileId, pageIndex);
  }

  @Override
  public synchronized void putToMRU(OCacheEntry cacheEntry) {
    underlying.putToMRU(cacheEntry);
  }

  @Override
  public synchronized void clear() {
    underlying.clear();
  }

  @Override
  public synchronized boolean contains(long fileId, long filePosition) {
    return underlying.contains(fileId, filePosition);
  }

  @Override
  public synchronized int size() {
    return underlying.size();
  }

  @Override
  public synchronized OCacheEntry removeLRU() {
    return underlying.removeLRU();
  }

  @Override
  public synchronized OCacheEntry getLRU() {
    return underlying.getLRU();
  }

  @Override
  public synchronized Iterator<OCacheEntry> iterator() {
    return underlying.iterator();
  }
}
