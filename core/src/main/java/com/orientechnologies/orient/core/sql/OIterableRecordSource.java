package com.orientechnologies.orient.core.sql;

import java.util.Iterator;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public interface OIterableRecordSource {
  Iterator<OIdentifiable> iterator(final Map<Object, Object> iArgs);
}
