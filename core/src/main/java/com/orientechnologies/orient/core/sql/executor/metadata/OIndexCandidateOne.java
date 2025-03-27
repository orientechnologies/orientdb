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
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidate.PropertyValue;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class OIndexCandidateOne implements OIndexCandidate {

  private final String name;
  private final List<PropertyValue> start;
  private final boolean forceDistinct;
  private final Optional<List<PropertyValue>> end;
  private Operation operation;

  public OIndexCandidateOne(
      String name, Operation operation, String prop, OIndexKeySource value, boolean forceDistinct) {
    this.name = name;
    this.operation = operation;
    this.forceDistinct = forceDistinct;
    this.start = Collections.singletonList(new PropertyValue(prop, value, operation));
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
      this.start = Collections.singletonList(new PropertyValue(prop, firstSource, firstOp));
      this.end =
          Optional.of(Collections.singletonList(new PropertyValue(prop, secondSource, secondOp)));
    } else {
      this.start = Collections.singletonList(new PropertyValue(prop, secondSource, secondOp));
      this.end =
          Optional.of(Collections.singletonList(new PropertyValue(prop, firstSource, firstOp)));
    }
    this.name = name;
    this.operation = Operation.Range;
    this.forceDistinct = forceDistinct;
  }

  public OIndexCandidateOne(OIndexCandidateOne first, OIndexCandidateOne second) {
    this(
        first.name,
        first.start.iterator().next().name(),
        first.start.iterator().next().operation(),
        first.start.iterator().next().source(),
        second.start.iterator().next().operation(),
        second.start.iterator().next().source(),
        false);
  }

  public OIndexCandidateOne(String index, List<PropertyValue> pv, List<PropertyValue> to) {
    this.name = index;
    this.forceDistinct = false;
    if (to.isEmpty()) {
      this.start = pv;
      this.end = Optional.empty();
    } else {
      if (pv.get(pv.size() - 1).operation().isG()) {
        this.start = pv;
        this.end = Optional.of(to);
      } else {
        this.start = to;
        this.end = Optional.of(pv);
      }
    }
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
    if (end.isPresent()) {
      return Operation.Range;
    }
    return start.get(start.size() - 1).operation();
  }

  public Optional<OIndexCandidate> normalize(OCommandContext ctx) {
    return Optional.of(this);
  }

  @Override
  public Optional<OIndexCandidate> finalize(OCommandContext ctx) {
    OIndexInternal index =
        ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.name).getInternal();
    List<String> fields = index.getDefinition().getFields();
    if (!index.supportsOrderedIterations() && start.size() != fields.size()) {
      return Optional.empty();
    }
    if (start.size() <= fields.size()) {
      for (int i = 0; i < start.size(); i++) {
        if (!fields.get(i).equals(start.get(i).name())) {
          return Optional.empty();
        }
      }
      return Optional.of(this);
    }
    return Optional.empty();
  }

  @Override
  public List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndexInternal index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, name).getInternal();
    List<OIndexStream> streams = new ArrayList<>();
    if (this.end.isPresent()) {
      List<PropertyValue> endProps = this.end.get();
      boolean startInclude = this.start.get(this.start.size() - 1).operation().isInclude();
      boolean endInclude = endProps.get(endProps.size() - 1).operation().isInclude();
      Collection<Object> start = computeValues(this.start, ctx, isOrderAsc);
      Collection<Object> end = computeValues(endProps, ctx, isOrderAsc);
      end.toString();
      streams.add(new OBetweenIndexStream(index, start, startInclude, end, endInclude, isOrderAsc));
    } else {
      Collection<Object> val = computeValues(this.start, ctx, isOrderAsc);
      if (val == null) {
        streams.add(new ONullIndexStream(index));
      } else {
        for (Object singleVal : val) {
          switch (getOperation()) {
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

  private Collection<Object> computeValues(
      List<PropertyValue> values, OCommandContext ctx, boolean isOrderAsc) {
    List<List<Object>> fields = new ArrayList<>();
    for (PropertyValue source : values) {
      fields.add(new ArrayList<Object>(source.source().key(ctx, isOrderAsc)));
    }
    List<List<Object>> keys = cartesianProduct(fields);
    return (Collection) keys;
  }

  public static List<List<Object>> cartesianProduct(List<List<Object>> fields) {
    List<List<Object>> product = new ArrayList<>();
    LinkedList<Object> stack = new LinkedList<>();
    LinkedList<Integer> xcursor = new LinkedList<>();
    int x = 0;
    int y = 0;
    while (fields.size() > y && fields.get(y).size() > x) {
      xcursor.addLast(x);
      stack.addLast(fields.get(y).get(x));
      if (fields.size() - 1 == y) {
        product.add(new ArrayList<>(stack));
        stack.removeLast();
        Integer lastx = xcursor.removeLast();
        assert x == lastx;
        x++;
        if (x == fields.get(y).size()) {
          while (x == fields.get(y).size() && !xcursor.isEmpty()) {
            x = xcursor.removeLast();
            x++;
            y -= 1;
            stack.removeLast();
          }
          if (xcursor.isEmpty() && x == fields.get(y).size()) {
            break;
          }
        }
      } else {
        y++;
        x = 0;
      }
    }
    return product;
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
          boolean foundProperty = false;
          for (PropertyValue prop : start) {
            if (prop.name().equals(field)) {
              foundProperty = true;
              break;
            }
          }
          if (!foundProperty) {
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
    return this.start.stream().map(PropertyValue::name).toList();
  }

  @Override
  public List<PropertyValue> values() {
    return start;
  }

  @Override
  public List<PropertyValue> toValues() {
    return end.orElseGet(Collections::emptyList);
  }
}
