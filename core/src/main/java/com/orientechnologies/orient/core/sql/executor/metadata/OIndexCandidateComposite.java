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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OIndexCandidateComposite implements OIndexCandidate {
  private String index;
  private Operation operation;
  private List<String> properties;
  private List<OIndexKeySource> values;
  private boolean forceDistinct = false;
  private final List<PropertyValue> propValues;

  public OIndexCandidateComposite(
      String index, Operation operation, List<String> properties, List<OIndexKeySource> value) {
    this.index = index;
    this.operation = operation;
    this.properties = properties;
    this.values = value;
    List<PropertyValue> pv = new ArrayList<>();
    for (int i = 0; i < properties.size(); i++) {
      String prop = properties.get(i);
      OIndexKeySource val = value.get(i);
      pv.add(new PropertyValue(prop, val));
    }
    this.propValues = pv;
  }

  public OIndexCandidateComposite(
      String index,
      Operation operation,
      String property,
      OIndexKeySource value,
      boolean forceDistinct) {
    this.index = index;
    this.operation = operation;
    this.properties = Collections.singletonList(property);
    this.values = Collections.singletonList(value);
    this.forceDistinct = forceDistinct;
    this.propValues = Collections.singletonList(new PropertyValue(property, value));
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
  public List<String> properties() {
    return properties;
  }

  public boolean requiresDistinctStep(OCommandContext ctx) {
    if (forceDistinct) {
      return true;
    }
    OIndex index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.index);
    OIndexDefinition def = index.getDefinition();
    if (def instanceof OCompositeIndexDefinition
        && ((OCompositeIndexDefinition) def).getMultiValueDefinition() != null) {
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

    Collection<Object> val = computeValues(ctx, isOrderAsc);
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
            } else if (index.supportsOrderedIterations()) {
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
    return streams;
  }

  private Collection<Object> computeValues(OCommandContext ctx, boolean isOrderAsc) {
    List<List<Object>> fields = new ArrayList<>();
    for (OIndexKeySource source : values) {
      fields.add(new ArrayList<Object>(source.key(ctx, isOrderAsc)));
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
    while (true) {
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

  @Override
  public Optional<OIndexCandidate> finalize(OCommandContext ctx) {
    OIndexInternal index =
        ctx.getDatabase().getMetadata().getIndexManager().getIndex(this.index).getInternal();
    List<String> fields = index.getDefinition().getFields();
    if (!index.supportsOrderedIterations() && properties.size() != fields.size()) {
      return Optional.empty();
    }
    if (properties.size() <= fields.size()) {
      for (int i = 0; i < properties.size(); i++) {
        if (!fields.get(i).equals(properties.get(i))) {
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
    int foundOrd = 0;
    if (orderItems.size() <= fields.size()) {
      for (String field : fields) {
        if (orderItems.contains(field)) {
          foundOrd++;
        } else if (foundOrd == 0) {
          boolean foundProperty = false;
          for (String prop : properties) {
            if (prop.equals(field)) {
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

  public Map<String, OIndexKeySource> mappedValues() {
    Map<String, OIndexKeySource> sources = new HashMap<>();
    for (int i = 0; i < values.size(); i++) {
      sources.put(this.properties.get(i), this.values.get(i));
    }
    return sources;
  }

  @Override
  public List<PropertyValue> values() {
    return propValues;
  }
}
