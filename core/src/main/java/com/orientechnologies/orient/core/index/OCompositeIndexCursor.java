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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.*;

/**
 * @author Andrey Lomakin
 */
public class OCompositeIndexCursor extends OIndexAbstractCursor {
  private Collection<OIndexCursor> cursors;
  private Iterator<OIndexCursor>   cursorIterator;
  private OIndexCursor             cursor;

  public OCompositeIndexCursor(Collection<OIndexCursor> cursors) {
    this.cursors = cursors;

    cursorIterator = this.cursors.iterator();
    if (cursorIterator.hasNext())
      cursor = cursorIterator.next();
  }

  @Override
  public Map.Entry<Object, OIdentifiable> nextEntry() {
    Map.Entry<Object, OIdentifiable> entry = null;

    while (entry == null && cursor != null) {
      entry = cursor.nextEntry();

      if (entry == null) {
        if (cursorIterator.hasNext()) {
          cursor = cursorIterator.next();
        } else {
          cursor = null;
        }
      }
    }

    return entry;
  }
}
