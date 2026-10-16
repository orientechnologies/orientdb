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
  private final boolean empty;

  public OExactIndexStream(OIndexInternal index, Object startKey, boolean asc) {
    super();
    this.index = index;
    this.empty =
        index.getDefinition().getFields().size() == 1
            && startKey instanceof Collection
            && ((Collection<?>) startKey).isEmpty();
    this.startKey =
        this.empty ? Collections.emptyList() : toIndexKey(index.getDefinition(), startKey);
    this.asc = asc;
  }

  private static Collection<Object> toIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      // IN (subquery) feeds one row's value per stream; an empty-list row contributes no key.
      if (((Collection) rightValue).isEmpty()) {
        rightValue = null;
      } else {
        rightValue = ((Collection) rightValue).iterator().next();
      }
    }
    if (rightValue instanceof List) {
      rightValue = definition.createValue((List<?>) rightValue);
    } else if (!(rightValue instanceof OCompositeKey)) {
      rightValue = definition.createValue(rightValue);
    }
    if (!(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return (Collection<Object>) rightValue;
  }

  public Stream<ORawPair<Object, ORID>> start(OCommandContext ctx) {
    if (empty) {
      return Stream.empty();
    }
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
