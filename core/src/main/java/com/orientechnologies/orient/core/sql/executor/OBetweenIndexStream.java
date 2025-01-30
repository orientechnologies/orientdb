package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.Collection;
import java.util.stream.Stream;

public class OBetweenIndexStream implements OIndexStream {
  private OIndexInternal index;
  private Object startKey;
  private boolean includeStart;
  private Object endKey;
  private boolean includeEnd;
  private boolean asc;

  public OBetweenIndexStream(
      OIndexInternal index,
      Object startKey,
      boolean includeStart,
      Object endKey,
      boolean includeEnd,
      boolean asc) {
    super();
    this.index = index;
    this.startKey = toBetweenIndexKey(index.getDefinition(), startKey);
    this.includeStart = includeStart;
    this.endKey = toBetweenIndexKey(index.getDefinition(), endKey);
    this.includeEnd = includeEnd;
    this.asc = asc;
  }

  private static Object toBetweenIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      if (((Collection) rightValue).size() > 0) {
        rightValue = ((Collection) rightValue).iterator().next();
      } else {
        rightValue = null;
      }
    }

    if (rightValue instanceof Collection) {
      rightValue = definition.createValue(((Collection) rightValue).toArray());
    } else {
      rightValue = definition.createValue(rightValue);
    }

    return rightValue;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    if (startKey == null && endKey == null) {
      final Stream<ORID> stream = index.getRids(null);
      return stream.map((rid) -> new ORawPair<>(null, rid));
    } else {
      return index.streamEntriesBetween(startKey, includeStart, endKey, includeEnd, asc);
    }
  }

  public OIndexStreamStat indexStats() {
    if (startKey == null && endKey == null) {
      return new OIndexStreamStat(index.getName(), index.getDefinition().getParamCount(), 2);
    } else {
      int keySize;
      if (this.startKey instanceof OCompositeKey) {
        keySize = ((OCompositeKey) this.startKey).getKeys().size();
      } else {
        keySize = 1;
      }
      return new OIndexStreamStat(index.getName(), index.getDefinition().getParamCount(), keySize);
    }
  }
}
