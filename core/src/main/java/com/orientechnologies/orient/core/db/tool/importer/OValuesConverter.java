package com.orientechnologies.orient.core.db.tool.importer;

/** Created by tglman on 28/07/17. */
public interface OValuesConverter<T> {
  T convert(T value);
}
