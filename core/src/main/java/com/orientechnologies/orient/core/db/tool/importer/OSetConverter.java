package com.orientechnologies.orient.core.db.tool.importer;

import java.util.HashSet;
import java.util.Set;

/** Created by tglman on 28/07/17. */
public final class OSetConverter extends OAbstractCollectionConverter<Set> {
  public OSetConverter(OConverterData converterData) {
    super(converterData);
  }

  @Override
  public Set convert(Set value) {
    boolean updated = false;
    final Set result;

    result = new HashSet();

    final ResultCallback callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add(item);
          }
        };

    for (Object item : value) updated = convertSingleValue(item, callback, updated);

    if (updated) return result;

    return value;
  }
}
