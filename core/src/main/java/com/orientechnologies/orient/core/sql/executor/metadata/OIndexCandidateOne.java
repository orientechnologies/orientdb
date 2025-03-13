package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
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

public class OIndexCandidateOne implements OIndexCandidate {

  private final String name;
  private final OProperty property;
  private final OIndexKeySource value;
  private Operation operation;

  public OIndexCandidateOne(
      String name, Operation operation, OProperty prop, OIndexKeySource value) {
    this.name = name;
    this.operation = operation;
    this.property = prop;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  @Override
  public Optional<OIndexCandidate> invert() {
    if (this.operation == Operation.Ge) {
      this.operation = Operation.Lt;
    } else if (this.operation == Operation.Gt) {
      this.operation = Operation.Le;
    } else if (this.operation == Operation.Le) {
      this.operation = Operation.Gt;
    } else if (this.operation == Operation.Lt) {
      this.operation = Operation.Ge;
    }
    return Optional.of(this);
  }

  public Operation getOperation() {
    return operation;
  }

  public OIndexKeySource getValue() {
    return value;
  }

  @Override
  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(name);
    if (property.getName().equals(index.getDefinition().getFields().get(0))) {
      return Optional.of(this);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndexInternal index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, name).getInternal();
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

  public boolean requiresDistinctStep(OCommandContext ctx) {
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(name);
    if (index instanceof OCompositeIndexDefinition
        && ((OCompositeIndexDefinition) index.getDefinition()).getMultiValueDefinition() != null) {
      return true;
    }
    return false;
  }

  public boolean fullySorted(List<String> orderItems, OCommandContext ctx) {
    if (orderItems.size() == 1 && orderItems.get(0).equals(property.getName())) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public List<OIndexKeySource> values() {
    return Collections.singletonList(value);
  }

  @Override
  public List<OProperty> properties() {
    return Collections.singletonList(this.property);
  }
}
