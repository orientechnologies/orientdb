/*
 * Copyright 2013 Orient Technologies.
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.method.misc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Transforms current value in a Map.
 * 
 * @author Luca Garulli
 */
public class OSQLMethodAsMap extends OAbstractSQLMethod {

  public static final String NAME = "asmap";

  public OSQLMethodAsMap() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iMethodParams) {
    if (ioResult instanceof Map)
      // ALREADY A MAP
      return ioResult;

    if (ioResult == null)
      // NULL VALUE, RETURN AN EMPTY SET
      return new HashMap<Object, Object>();

    Iterator<Object> iter;
    if (ioResult instanceof Iterator<?>)
      iter = (Iterator<Object>) ioResult;
    else if (ioResult instanceof Iterable<?>)
      iter = ((Iterable<Object>) ioResult).iterator();
    else
      return null;

    final HashMap<Object, Object> map = new HashMap<Object, Object>();
    while (iter.hasNext()) {
      final Object key = iter.next();
      if (iter.hasNext()) {
        final Object value = iter.next();
        map.put(key, value);
      }
    }

    return map;
  }
}
