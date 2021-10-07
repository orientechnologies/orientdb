package com.orientechnologies.orient.core.db.tool.importer;

import java.util.LinkedHashMap;
import java.util.Map;

/** Created by tglman on 28/07/17. */
public final class OMapConverter extends OAbstractCollectionConverter<Map> {
  public OMapConverter(OConverterData converterData) {
    super(converterData);
  }

  @Override
  public Map convert(Map value) {
    final Map result = new LinkedHashMap();
    boolean updated = false;
    final class MapResultCallback implements ResultCallback {
      private Object key;

      @Override
      public void add(Object item) {
        result.put(key, item);
      }

      public void setKey(Object key) {
        this.key = key;
      }
    }

    final MapResultCallback callback = new MapResultCallback();
    for (Map.Entry entry : (Iterable<Map.Entry>) value.entrySet()) {
      callback.setKey(entry.getKey());
      updated = convertSingleValue(entry.getValue(), callback, updated) || updated;
    }
    if (updated) return result;

    return value;
  }
}
