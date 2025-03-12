package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.sql.executor.OExactIndexStream;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.OMajorIndexStream;
import com.orientechnologies.orient.core.sql.executor.OMinorIndexStream;
import com.orientechnologies.orient.core.sql.executor.ONullIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OIndexCandidateComposite implements OIndexCandidate {
  private String index;
  private Operation operation;
  private List<OProperty> properties;
  private OIndexKeySource value;

  public OIndexCandidateComposite(
      String index, Operation operation, List<OProperty> properties, OIndexKeySource value) {
    this.index = index;
    this.operation = operation;
    this.properties = properties;
    this.value = value;
  }

  public OIndexCandidateComposite(
      String index, Operation operation, OProperty property, OIndexKeySource value) {
    this.index = index;
    this.operation = operation;
    this.properties = Collections.singletonList(property);
    this.value =
        (ctx) -> {
          OCompositeKey key = new OCompositeKey();
          key.addKey(value.key(ctx));
          return key;
        };
  }

  @Override
  public String getName() {
    return index;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    return Optional.empty();
  }

  @Override
  public Operation getOperation() {
    return operation;
  }

  @Override
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public List<OProperty> properties() {
    return properties;
  }

  @Override
  public List<OIndexKeySource> values() {
    return Collections.singletonList(value);
  }

  public boolean requiresDistinctStep(OCommandContext ctx) {
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.index);
    if (index instanceof OCompositeIndexDefinition
        && ((OCompositeIndexDefinition) index.getDefinition()).getMultiValueDefinition() != null) {
      return true;
    }
    return false;
  }

  @Override
  public List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndexInternal index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, this.index)
            .getInternal();
    Object val = value.key(ctx);
    switch (operation) {
      case Ge:
        return Collections.singletonList(new OMajorIndexStream(index, val, true, isOrderAsc));
      case Gt:
        return Collections.singletonList(new OMajorIndexStream(index, val, false, isOrderAsc));
      case Le:
        return Collections.singletonList(new OMinorIndexStream(index, val, true, isOrderAsc));
      case Lt:
        return Collections.singletonList(new OMinorIndexStream(index, val, false, isOrderAsc));
      case Eq:
        if (val == null) {
          return Collections.singletonList(new ONullIndexStream(index));
        } else {
          return Collections.singletonList(new OExactIndexStream(index, val, isOrderAsc));
        }

      default:
        throw new UnsupportedOperationException("unsupported operation " + operation);
    }
  }

  public boolean fullySorted(List<String> orderItems) {
    // TODO: check  if properties are unique
    List<OProperty> properties = this.properties();
    if (orderItems.size() == properties.size()) {
      Set<String> set = properties.stream().map((x) -> x.getName()).collect(Collectors.toSet());
      return set.containsAll(orderItems);
    } else {
      return false;
    }
  }
}
