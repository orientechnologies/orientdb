package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 4/24/14
 */
public abstract class OIndexAbstractCursor implements OIndexCursor {

  @Override
  public Set<OIdentifiable> toValues() {
    HashSet<OIdentifiable> result = new HashSet<OIdentifiable>();
    Map.Entry<Object, OIdentifiable> entry = next(-1);

    while (entry != null) {
      result.add(entry.getValue());
      entry = next(-1);
    }

    return result;
  }

  @Override
  public Set<Map.Entry<Object, OIdentifiable>> toEntries() {
    HashSet<Map.Entry<Object, OIdentifiable>> result = new HashSet<Map.Entry<Object, OIdentifiable>>();

    Map.Entry<Object, OIdentifiable> entry = next(-1);

    while (entry != null) {
      result.add(entry);
      entry = next(-1);
    }

    return result;
  }

  @Override
  public Set<Object> toKeys() {
    HashSet<Object> result = new HashSet<Object>();

    Map.Entry<Object, OIdentifiable> entry = next(-1);

    while (entry != null) {
      result.add(entry.getKey());
      entry = next(-1);
    }

    return result;
  }
}