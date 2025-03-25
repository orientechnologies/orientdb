package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class OMajorIndexStream implements OIndexStream {
  private OIndexInternal index;
  private Object startKey;
  private boolean include;
  private boolean asc;

  public OMajorIndexStream(OIndexInternal index, Object startKey, boolean include, boolean asc) {
    super();
    this.index = index;
    this.startKey = toIndexKey(index.getDefinition(), startKey);
    this.include = include;
    this.asc = asc;
  }

  private static Object toIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection) rightValue).iterator().next();
    }
    if (rightValue instanceof List) {
      rightValue = definition.createValue((List<?>) rightValue);
    } else if (!(rightValue instanceof OCompositeKey)) {
      rightValue = definition.createValue(rightValue);
    }
    return rightValue;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    return index.streamEntriesMajor(startKey, include, asc);
  }

  public OIndexStreamStat indexStats() {
    int keySize;
    if (this.startKey instanceof OCompositeKey) {
      keySize = ((OCompositeKey) this.startKey).getKeys().size();
    } else {
      keySize = 1;
    }
    return new OIndexStreamStat(index.getName(), index.getDefinition().getParamCount(), keySize);
  }
}
