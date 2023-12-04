package com.orientechnologies.orient.core.index.iterator;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class OIndexCursorStream extends OIndexAbstractCursor {
  private final Iterator<ORawPair<Object, ORID>> iterator;

  public OIndexCursorStream(final Stream<ORawPair<Object, ORID>> stream) {
    iterator = stream.iterator();
  }

  @Override
  public Map.Entry<Object, OIdentifiable> nextEntry() {
    if (iterator.hasNext()) {
      final ORawPair<Object, ORID> pair = iterator.next();

      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return pair.first;
        }

        @Override
        public OIdentifiable getValue() {
          return pair.second;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException();
        }
      };
    }

    return null;
  }
}
