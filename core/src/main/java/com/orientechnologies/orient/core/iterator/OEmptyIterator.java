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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.orient.core.id.ORID;
import java.util.Iterator;

/** Empty iterator against Object. */
public class OEmptyIterator<T> implements Iterator<T> {
  public static final OEmptyIterator<Object> ANY_INSTANCE = new OEmptyIterator<>();
  public static final OEmptyIterator<ORID> IDENTIFIABLE_INSTANCE = new OEmptyIterator<>();

  public boolean hasNext() {
    return false;
  }

  public T next() {
    return null;
  }

  public void remove() {}
}
