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

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
class LRUEntry {
  OCacheEntry cacheEntry;

  long        hashCode;

  LRUEntry    next;

  LRUEntry    after;
  LRUEntry    before;

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    LRUEntry lruEntry = (LRUEntry) o;

    if (!cacheEntry.equals(lruEntry.cacheEntry))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return cacheEntry.hashCode();
  }

  @Override
  public String toString() {
    return "LRUEntry{" + "cacheEntry=" + cacheEntry + ", hashCode=" + hashCode + '}';
  }
}
