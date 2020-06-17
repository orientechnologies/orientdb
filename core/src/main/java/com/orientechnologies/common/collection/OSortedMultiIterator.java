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
package com.orientechnologies.common.collection;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OOrderBy;
import com.orientechnologies.orient.core.sql.parser.OOrderByItem;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OSortedMultiIterator<T extends OIdentifiable> implements Iterator<T> {

  private static final int STATUS_INIT = 0;
  private static final int STATUS_RUNNING = 1;

  private final OOrderBy orderBy;

  private List<Iterator<T>> sourceIterators = new ArrayList<Iterator<T>>();
  private List<T> heads = new ArrayList<T>();

  private int status = STATUS_INIT;

  public OSortedMultiIterator(OOrderBy orderBy) {
    this.orderBy = orderBy;
  }

  public void add(Iterator<T> iterator) {
    if (status == STATUS_INIT) {
      sourceIterators.add(iterator);
      if (iterator.hasNext()) {
        heads.add(iterator.next());
      } else {
        heads.add(null);
      }
    } else {
      throw new IllegalStateException(
          "You are trying to add a sub-iterator on a running OSortedMultiIterator");
    }
  }

  @Override
  public boolean hasNext() {
    if (status == STATUS_INIT) {
      status = STATUS_RUNNING;
    }
    for (T o : heads) {
      if (o != null) {
        return true;
      }
    }
    return false;
  }

  @Override
  public T next() {
    if (status == STATUS_INIT) {
      status = STATUS_RUNNING;
    }
    int nextItemPosition = findNextPosition();
    T result = heads.get(nextItemPosition);
    if (sourceIterators.get(nextItemPosition).hasNext()) {
      heads.set(nextItemPosition, sourceIterators.get(nextItemPosition).next());
    } else {
      heads.set(nextItemPosition, null);
    }
    return result;
  }

  private int findNextPosition() {
    int lastPosition = 0;
    while (heads.size() < lastPosition && heads.get(lastPosition) == null) {
      lastPosition++;
    }
    T lastItem = heads.get(lastPosition);
    for (int i = lastPosition + 1; i < heads.size(); i++) {
      T item = heads.get(i);
      if (item == null) {
        continue;
      }
      if (comesFrist(item, lastItem)) {
        lastItem = item;
        lastPosition = i;
      }
    }
    return lastPosition;
  }

  protected boolean comesFrist(T left, T right) {
    if (orderBy == null || orderBy.getItems() == null || orderBy.getItems().size() == 0) {
      return true;
    }
    if (right == null) {
      return true;
    }
    if (left == null) {
      return false;
    }

    ODocument leftDoc =
        (left instanceof ODocument) ? (ODocument) left : (ODocument) left.getRecord();
    ODocument rightDoc =
        (right instanceof ODocument) ? (ODocument) right : (ODocument) right.getRecord();

    for (OOrderByItem orderItem : orderBy.getItems()) {
      Object leftVal = leftDoc.field(orderItem.getRecordAttr());
      Object rightVal = rightDoc.field(orderItem.getRecordAttr());
      if (rightVal == null) {
        return true;
      }
      if (leftVal == null) {
        return false;
      }
      if (leftVal instanceof Comparable) {
        int compare = ((Comparable) leftVal).compareTo(rightVal);
        if (compare == 0) {
          continue;
        }
        boolean greater = compare > 0;
        if (OOrderByItem.DESC.equals(orderItem.getType())) {
          return greater;
        } else {
          return !greater;
        }
      }
    }

    return false;
  }

  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
