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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
          return (Collection)
              value.key(ctx).stream()
                  .map(
                      (v) -> {
                        return new OCompositeKey(v);
                      })
                  .toList();
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
    Collection<Object> val = value.key(ctx);
    List<OIndexStream> streams = new ArrayList<>();
    if (val == null) {
      streams.add(new ONullIndexStream(index));
    } else {
      for (Object singleVal : val) {
        switch (operation) {
          case Ge:
            streams.add(new OMajorIndexStream(index, singleVal, true, isOrderAsc));
            break;
          case Gt:
            streams.add(new OMajorIndexStream(index, singleVal, false, isOrderAsc));
            break;
          case Le:
            streams.add(new OMinorIndexStream(index, singleVal, true, isOrderAsc));
            break;
          case Lt:
            streams.add(new OMinorIndexStream(index, singleVal, false, isOrderAsc));
            break;
          case Eq:
            if (singleVal == null) {
              streams.add(new ONullIndexStream(index));
            } else {
              streams.add(new OExactIndexStream(index, singleVal, isOrderAsc));
            }
            break;

          default:
            throw new UnsupportedOperationException("unsupported operation " + operation);
        }
      }
    }
    return streams;
  }

  @Override
  public Optional<OIndexCandidate> finalize(OCommandContext ctx) {
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.index);
    List<String> fields = index.getDefinition().getFields();
    if (properties.size() <= fields.size()) {
      for (int i = 0; i < properties.size(); i++) {
        if (!fields.get(i).equals(properties.get(i).getName())) {
          return Optional.empty();
        }
      }
      return Optional.of(this);
    }
    return Optional.empty();
  }

  public boolean fullySorted(List<String> orderItems, OCommandContext ctx) {
    // TODO: check  if properties are unique
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.index);
    List<String> fields = index.getDefinition().getFields();
    if (orderItems.size() <= fields.size()) {
      return fields.containsAll(orderItems);
    } else {
      return false;
    }
  }
}
