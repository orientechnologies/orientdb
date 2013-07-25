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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Transforms current value in a Set.
 * 
 * @author Luca Garulli
 */
public class OSQLMethodAsSet extends OAbstractSQLMethod {

  public static final String NAME = "asset";

  public OSQLMethodAsSet() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iMethodParams) {
    if (ioResult instanceof Set)
      // ALREADY A SET
      return ioResult;

    if (ioResult == null)
      // NULL VALUE, RETURN AN EMPTY SET
      return new HashSet<Object>();

    if (ioResult instanceof Collection<?>)
      return new HashSet<Object>((Collection<Object>) ioResult);
    else if (ioResult instanceof Iterable<?>)
      ioResult = ((Iterable<?>) ioResult).iterator();

    if (ioResult instanceof Iterator<?>) {
      final Set<Object> set = ioResult instanceof OSizeable ? new HashSet<Object>(((OSizeable) ioResult).size())
          : new HashSet<Object>();

      for (Iterator<Object> iter = (Iterator<Object>) ioResult; iter.hasNext();)
        set.add(iter.next());
      return set;
    }

    // SINGLE ITEM: ADD IT AS UNIQUE ITEM
    final Set<Object> set = new HashSet<Object>();
    set.add(ioResult);
    return set;
  }
}
