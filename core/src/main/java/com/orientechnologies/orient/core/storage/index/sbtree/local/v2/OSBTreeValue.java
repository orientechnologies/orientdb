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

package com.orientechnologies.orient.core.storage.index.sbtree.local.v2;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 9/27/13
 */
public class OSBTreeValue<V> {
  private final boolean isLink;
  private final long link;
  private final V value;

  public OSBTreeValue(boolean isLink, long link, V value) {
    this.isLink = isLink;
    this.link = link;
    this.value = value;
  }

  public boolean isLink() {
    return isLink;
  }

  public long getLink() {
    return link;
  }

  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OSBTreeValue that = (OSBTreeValue) o;

    if (isLink != that.isLink) return false;
    if (link != that.link) return false;
    if (value != null ? !value.equals(that.value) : that.value != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (isLink ? 1 : 0);
    result = 31 * result + (int) (link ^ (link >>> 32));
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OSBTreeValue{" + "isLink=" + isLink + ", link=" + link + ", value=" + value + '}';
  }
}
