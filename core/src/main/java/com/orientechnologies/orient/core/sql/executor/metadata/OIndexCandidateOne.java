package com.orientechnologies.orient.core.sql.executor.metadata;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.executor.OBetweenIndexStream;
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
  private final String property;
  private final PropertyValue start;
  private final boolean forceDistinct;
  private final Optional<PropertyValue> end;
  private Operation operation;

  public OIndexCandidateOne(
      String name, Operation operation, String prop, OIndexKeySource value, boolean forceDistinct) {
    this.name = name;
    this.operation = operation;
    this.property = prop;
    this.forceDistinct = forceDistinct;
    this.start = new PropertyValue(prop, value, operation);
    this.end = Optional.empty();
  }

  public OIndexCandidateOne(
      String name,
      String prop,
      Operation firstOp,
      OIndexKeySource firstSource,
      Operation secondOp,
      OIndexKeySource secondSource,
      boolean forceDistinct) {

    if (firstOp.isG()) {
      this.start = new PropertyValue(prop, firstSource, firstOp);
      this.end = Optional.of(new PropertyValue(prop, secondSource, secondOp));
    } else {
      this.start = new PropertyValue(prop, secondSource, secondOp);
      this.end = Optional.of(new PropertyValue(prop, firstSource, firstOp));
    }
    this.name = name;
    this.operation = Operation.Range;
    this.property = prop;
    this.forceDistinct = forceDistinct;
  }

  public OIndexCandidateOne(OIndexCandidateOne first, OIndexCandidateOne second) {
    this(
        first.name,
        first.property,
        first.start.operation(),
        first.start.source(),
        second.start.operation(),
        second.start.source(),
        false);
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
    return start.source();
  }

  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public Optional<OIndexCandidate> finalize(OCommandContext ctx) {
    OIndexInternal index =
        ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.name).getInternal();
    List<String> fields = index.getDefinition().getFields();
    if (!index.supportsOrderedIterations() && fields.size() != 1) {
      return Optional.empty();
    }
    if (property.equals(index.getDefinition().getFields().get(0))) {
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
    List<OIndexStream> streams = new ArrayList<>();
    if (this.end.isPresent()) {
      streams.add(
          new OBetweenIndexStream(
              index,
              start.source().key(ctx, isOrderAsc).iterator().next(),
              start.operation().isInclude(),
              end.get().source().key(ctx, isOrderAsc).iterator().next(),
              end.get().operation().isInclude(),
              isOrderAsc));
    } else {
      Collection<Object> val = start.source().key(ctx, isOrderAsc);
      if (val == null) {
        streams.add(new ONullIndexStream(index));
      } else {
        for (Object singleVal : val) {
          switch (start.operation()) {
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
              } else if (index.supportsOrderedIterations()
                  && index.getDefinition().getFields().size() > 1) {
                streams.add(
                    new OBetweenIndexStream(index, singleVal, true, singleVal, true, isOrderAsc));
              } else {
                streams.add(new OExactIndexStream(index, singleVal, isOrderAsc));
              }
              break;
            default:
              throw new UnsupportedOperationException("unsupported operation " + operation);
          }
        }
      }
    }
    return streams;
  }

  public boolean requiresDistinctStep(OCommandContext ctx) {
    if (forceDistinct) {
      return true;
    }
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.name);
    OIndexDefinition def = index.getDefinition();
    if (def instanceof OCompositeIndexDefinition
        && ((OCompositeIndexDefinition) def).getMultiValueDefinition() != null) {
      return true;
    }
    return false;
  }

  public boolean fullySorted(List<String> orderItems, OCommandContext ctx) {
    // TODO: check  if properties are unique
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.name);
    List<String> fields = index.getDefinition().getFields();
    int foundOrd = 0;
    if (orderItems.size() <= fields.size()) {
      for (String field : fields) {
        if (orderItems.contains(field)) {
          foundOrd++;
        } else if (foundOrd == 0) {
          if (!property.equals(field)) {
            return false;
          }
        } else {
          return false;
        }
        if (foundOrd == orderItems.size()) {
          break;
        }
      }
      return foundOrd == orderItems.size();
    } else {
      return false;
    }
  }

  @Override
  public List<String> properties() {
    return Collections.singletonList(this.property);
  }

  @Override
  public List<PropertyValue> values() {
    return Collections.singletonList(start);
  }
}
