package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class OExactIndexStream implements OIndexStream {
  private OIndexInternal index;
  private Collection<Object> startKey;
  private boolean asc;

  public OExactIndexStream(OIndexInternal index, Object startKey, boolean asc) {
    super();
    this.index = index;
    this.startKey = toIndexKey(index.getDefinition(), startKey);
    this.asc = asc;
  }

  private static Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection) rightValue).iterator().next();
    }
    if (rightValue instanceof List) {
      rightValue = definition.createValue((List<?>) rightValue);
    } else if (!(rightValue instanceof OCompositeKey)) {
      rightValue = definition.createValue(rightValue);
    }
    if (!(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return (Collection) rightValue;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    return index.streamEntries(startKey, asc);
  }

  public OIndexStreamStat indexStats() {
    int keySize;
    if (this.startKey.size() > 0 && this.startKey.iterator().next() instanceof OCompositeKey) {
      keySize = ((OCompositeKey) this.startKey.iterator().next()).getKeys().size();
    } else {
      keySize = 1;
    }
    return new OIndexStreamStat(index.getName(), index.getDefinition().getParamCount(), keySize);
  }
}
