package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.common.log.OLogManager;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;

/** Created by Enrico Risa on 08/08/2017. */
public class OrientGraphQueryBuilder {

  private final boolean vertexStep;
  private List<String> classes = new ArrayList<>();

  private Map<String, P<?>> params = new LinkedHashMap<>();

  public OrientGraphQueryBuilder(boolean vertexStep) {
    this.vertexStep = vertexStep;
  }

  public OrientGraphQueryBuilder addCondition(HasContainer condition) {

    if (isLabelKey(condition.getKey())) {
      Object value = condition.getValue();
      if (value instanceof List) {
        ((List) value).forEach(label -> addClass((String) label));
      } else {
        addClass((String) value);
      }
    } else {
      params.put(condition.getKey(), condition.getPredicate());
    }
    return this;
  }

  private void addClass(String classLabel) {
    if (!classes.contains(classLabel)) {
      classes.add(classLabel);
    }
  }

  public Optional<OrientGraphBaseQuery> build(OGraph graph) {
    if (classes.size() == 0) {
      classes.add(vertexStep ? "V" : "E");
    } else {
      classes = classes.stream().filter(graph::existClass).collect(Collectors.toList());
      if (classes.size() == 0) {
        return Optional.of(new OrientGraphEmptyQuery());
      }
    }

    try {
      StringBuilder builder = new StringBuilder();

      Map<String, Object> parameters = new HashMap<>();
      String whereCondition = fillParameters(parameters);
      if (classes.size() > 1) {
        builder.append("SELECT expand($union) ");
        String lets =
            classes.stream()
                .map(
                    (s) ->
                        buildLetStatement(buildSingleQuery(s, whereCondition), classes.indexOf(s)))
                .reduce("", (a, b) -> a.isEmpty() ? b : a + " , " + b);
        builder.append(
            String.format("%s , $union = UNIONALL(%s)", lets, buildVariables(classes.size())));
      } else {
        builder.append(buildSingleQuery(classes.get(0), whereCondition));
      }
      return Optional.of(new OrientGraphQuery(builder.toString(), parameters, classes.size()));
    } catch (UnsupportedOperationException e) {
      OLogManager.instance().debug(this, "Cannot generate a query from the traversal", e);
    }
    return Optional.empty();
  }

  private String fillParameters(Map<String, Object> parameters) {
    StringBuilder whereBuilder = new StringBuilder();

    if (params.size() > 0) {
      whereBuilder.append(" WHERE ");
      boolean[] first = {true};

      AtomicInteger paramNum = new AtomicInteger();
      params
          .entrySet()
          .forEach(
              (e) -> {
                String param = "param" + paramNum.getAndIncrement();
                String cond = formatCondition(e.getKey(), param, e.getValue());
                if (first[0]) {
                  whereBuilder.append(" " + cond);
                  first[0] = false;
                } else {
                  whereBuilder.append(" AND " + cond);
                }
                parameters.put(param, e.getValue().getValue());
              });
    }
    return whereBuilder.toString();
  }

  private String buildVariables(int size) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < classes.size(); i++) {
      builder.append(String.format("$q%d", i));
      if (i < classes.size() - 1) {
        builder.append(",");
      }
    }
    return builder.toString();
  }

  protected String buildLetStatement(String query, int idx) {
    StringBuilder builder = new StringBuilder();
    if (idx == 0) {
      builder.append("LET ");
    }
    builder.append(String.format("$q%d = (%s)", idx, query));
    return builder.toString();
  }

  protected String buildSingleQuery(String clazz, String whereCondition) {
    return String.format("SELECT FROM `%s` %s", clazz, whereCondition);
  }

  private String formatCondition(String field, String param, P<?> predicate) {

    if (T.id.getAccessor().equalsIgnoreCase(field)) {
      return String.format(" %s %s :%s", "@rid", formatPredicate(predicate), param);
    } else {
      return String.format(" `%s` %s :%s", field, formatPredicate(predicate), param);
    }
  }

  private String formatPredicate(P<?> cond) {

    if (cond.getBiPredicate() instanceof Compare) {
      Compare compare = (Compare) cond.getBiPredicate();

      String condition = null;
      switch (compare) {
        case eq:
          condition = "=";
          break;
        case gt:
          condition = ">";
          break;
        case gte:
          condition = ">=";
          break;
        case lt:
          condition = "<";
          break;
        case lte:
          condition = "<=";
          break;
        case neq:
          condition = "<>";
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Predicate %s not supported!", compare.name()));
      }
      return condition;
    } else if (cond.getBiPredicate() instanceof Contains) {
      Contains contains = (Contains) cond.getBiPredicate();
      String condition = null;
      switch (contains) {
        case within:
          condition = "IN";
          break;
        case without:
          condition = "NOT IN";
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Predicate %s not supported!", contains.name()));
      }
      return condition;
    }

    throw new UnsupportedOperationException("Predicate not supported!");
  }

  private boolean isLabelKey(String key) {
    try {
      return T.fromString(key) == T.label;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
