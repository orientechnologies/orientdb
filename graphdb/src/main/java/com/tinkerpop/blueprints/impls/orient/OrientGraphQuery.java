package com.tinkerpop.blueprints.impls.orient;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

/**
 * OrientDB implementation for Graph query.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientGraphQuery extends DefaultGraphQuery {

  protected static final char   SPACE              = ' ';
  protected static final String OPERATOR_DIFFERENT = "<>";
  protected static final String OPERATOR_NOT       = "not ";
  protected static final String OPERATOR_IS_NOT    = "is not";
  protected static final String OPERATOR_LET       = "<=";
  protected static final char   OPERATOR_LT        = '<';
  protected static final String OPERATOR_GTE       = ">=";
  protected static final char   OPERATOR_GT        = '>';
  protected static final String OPERATOR_EQUALS    = "=";
  protected static final String OPERATOR_IS        = "is";
  protected static final String OPERATOR_IN        = " in ";
  protected static final String OPERATOR_LIKE      = " like ";

  protected static final String QUERY_FILTER_AND   = " and ";
  protected static final String QUERY_FILTER_OR    = " or ";
  protected static final char   QUERY_STRING       = '\'';
  protected static final char   QUERY_SEPARATOR    = ',';
  protected static final char   COLLECTION_BEGIN   = '[';
  protected static final char   COLLECTION_END     = ']';
  protected static final char   PARENTHESIS_BEGIN  = '(';
  protected static final char   PARENTHESIS_END    = ')';
  protected static final String QUERY_LABEL_BEGIN  = " and label in [";
  protected static final String QUERY_LABEL_END    = "]";
  protected static final String QUERY_WHERE        = " where ";
  protected static final String QUERY_SELECT_FROM  = "select from ";
  protected static final String SKIP               = " SKIP ";
  protected static final String LIMIT              = " LIMIT ";
  protected static final String ORDERBY            = " ORDER BY ";

  protected String              fetchPlan;

  public int                    skip               = 0;
  public String                 orderBy            = "";
  public String                 orderByDir         = "desc";

  public class OrientGraphQueryIterable<T extends Element> extends DefaultGraphQueryIterable<T> {

    public OrientGraphQueryIterable(final boolean forVertex) {
      super(forVertex);
    }

    protected Set<String> getIndexedKeys(final Class<? extends Element> elementClass) {
      return ((OrientBaseGraph) graph).getIndexedKeys(elementClass, true);
    }
  }

  public OrientGraphQuery(final Graph iGraph) {
    super(iGraph);
  }

  public Query labels(final String... labels) {
    this.labels = labels;
    return this;
  }

  public Query skip(int cnt) {
    this.skip = cnt;
    return this;
  }

  public Query order(final String props) {
    this.order(props, orderByDir);
    return this;
  }

  public Query order(final String props, final String dir) {
    this.orderBy = props;
    this.orderByDir = dir;
    return this;
  }

  @Override
  public Iterable<Vertex> vertices() {
    if (limit == 0)
      return Collections.emptyList();

    if (((OrientBaseGraph) graph).getRawGraph().getTransaction().isActive())
      // INSIDE TRANSACTION QUERY DOESN'T SEE IN MEMORY CHANGES, UNTIL
      // SUPPORTED USED THE BASIC IMPL
      return new OrientGraphQueryIterable<Vertex>(true);

    final StringBuilder text = new StringBuilder();

    // GO DIRECTLY AGAINST E CLASS AND SUB-CLASSES
    text.append(QUERY_SELECT_FROM);

    if (((OrientBaseGraph) graph).isUseClassForVertexLabel() && labels != null && labels.length > 0) {
      // FILTER PER CLASS SAVING CHECKING OF LABEL PROPERTY
      if (labels.length == 1)
        // USE THE CLASS NAME
        text.append(OrientBaseGraph.encodeClassName(labels[0]));
      else {
        // MULTIPLE CLASSES NOT SUPPORTED DIRECTLY: CREATE A SUB-QUERY
        return new OrientGraphQueryIterable<Vertex>(true);
      }
    } else
      text.append(OrientVertex.CLASS_NAME);

    // APPEND ALWAYS WHERE
    text.append(QUERY_WHERE);
    manageFilters(text);
    if (!((OrientBaseGraph) graph).isUseClassForVertexLabel())
      manageLabels(text);

    if (orderBy.length() > 1) {
      text.append(ORDERBY);
      text.append(orderBy);
      text.append(" " + orderByDir + " ");
    }
    if (skip > 0 && skip < Long.MAX_VALUE) {
      text.append(SKIP);
      text.append(skip);
    }

    if (limit > 0 && limit < Long.MAX_VALUE) {
      text.append(LIMIT);
      text.append(limit);
    }

    final OSQLSynchQuery<OIdentifiable> query = new OSQLSynchQuery<OIdentifiable>(text.toString());

    if (fetchPlan != null)
      query.setFetchPlan(fetchPlan);

    return new OrientElementIterable<Vertex>(((OrientBaseGraph) graph), ((OrientBaseGraph) graph).getRawGraph().query(query));
  }

  @Override
  public Iterable<Edge> edges() {
    if (limit == 0)
      return Collections.emptyList();

    if (((OrientBaseGraph) graph).getRawGraph().getTransaction().isActive())
      // INSIDE TRANSACTION QUERY DOESN'T SEE IN MEMORY CHANGES, UNTIL
      // SUPPORTED USED THE BASIC IMPL
      return new OrientGraphQueryIterable<Edge>(false);

    if (((OrientBaseGraph) graph).isUseLightweightEdges())
      return new OrientGraphQueryIterable<Edge>(false);

    final StringBuilder text = new StringBuilder();

    // GO DIRECTLY AGAINST E CLASS AND SUB-CLASSES
    text.append(QUERY_SELECT_FROM);

    if (((OrientBaseGraph) graph).isUseClassForEdgeLabel() && labels != null && labels.length > 0) {
      // FILTER PER CLASS SAVING CHECKING OF LABEL PROPERTY
      if (labels.length == 1)
        // USE THE CLASS NAME
        text.append(OrientBaseGraph.encodeClassName(labels[0]));
      else {
        // MULTIPLE CLASSES NOT SUPPORTED DIRECTLY: CREATE A SUB-QUERY
        return new OrientGraphQueryIterable<Edge>(false);
      }
    } else
      text.append(OrientEdge.CLASS_NAME);

    // APPEND ALWAYS WHERE 1=1 TO MAKE CONCATENATING EASIER
    text.append(QUERY_WHERE);

    manageFilters(text);
    if (!((OrientBaseGraph) graph).isUseClassForEdgeLabel())
      manageLabels(text);

    final OSQLSynchQuery<OIdentifiable> query = new OSQLSynchQuery<OIdentifiable>(text.toString());

    if (fetchPlan != null)
      query.setFetchPlan(fetchPlan);

    if (limit > 0 && limit < Long.MAX_VALUE)
      query.setLimit((int) limit);

    return new OrientElementIterable<Edge>(((OrientBaseGraph) graph), ((OrientBaseGraph) graph).getRawGraph().query(query));
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public void setFetchPlan(final String fetchPlan) {
    this.fetchPlan = fetchPlan;
  }

  protected void manageLabels(final StringBuilder text) {
    if (labels != null && labels.length > 0) {
      // APPEND LABELS
      text.append(QUERY_LABEL_BEGIN);
      for (int i = 0; i < labels.length; ++i) {
        if (i > 0)
          text.append(QUERY_SEPARATOR);
        text.append(QUERY_STRING);
        text.append(labels[i]);
        text.append(QUERY_STRING);
      }
      text.append(QUERY_LABEL_END);
    }
  }

  @SuppressWarnings("unchecked")
  protected void manageFilters(final StringBuilder text) {
    boolean firstPredicate = true;
    for (HasContainer has : hasContainers) {
      if (!firstPredicate)
        text.append(QUERY_FILTER_AND);
      else
        firstPredicate = false;

      if (has.predicate instanceof Contains) {
        // IN AND NOT_IN
        if (has.predicate == Contains.NOT_IN) {
          text.append(OPERATOR_NOT);
          text.append(PARENTHESIS_BEGIN);
        }
        text.append(has.key);

        if (has.value instanceof String) {
          text.append(OPERATOR_LIKE);
          generateFilterValue(text, has.value);
        } else {
          text.append(OPERATOR_IN);
          text.append(COLLECTION_BEGIN);

          boolean firstItem = true;
          for (Object o : (Collection<Object>) has.value) {
            if (!firstItem)
              text.append(QUERY_SEPARATOR);
            else
              firstItem = false;
            generateFilterValue(text, o);
          }

          text.append(COLLECTION_END);
        }

        if (has.predicate == Contains.NOT_IN)
          text.append(PARENTHESIS_END);
      } else {
        // ANY OTHER OPERATORS
        text.append(has.key);
        text.append(SPACE);

        if (has.predicate instanceof com.tinkerpop.blueprints.Compare) {
          final com.tinkerpop.blueprints.Compare compare = (com.tinkerpop.blueprints.Compare) has.predicate;
          switch (compare) {
          case EQUAL:
            if (has.value == null)
              // IS
              text.append(OPERATOR_IS);
            else
              // EQUALS
              text.append(OPERATOR_EQUALS);
            break;
          case GREATER_THAN:
            text.append(OPERATOR_GT);
            break;
          case GREATER_THAN_EQUAL:
            text.append(OPERATOR_GTE);
            break;
          case LESS_THAN:
            text.append(OPERATOR_LT);
            break;
          case LESS_THAN_EQUAL:
            text.append(OPERATOR_LET);
            break;
          case NOT_EQUAL:
            if (has.value == null)
              text.append(OPERATOR_IS_NOT);
            else
              text.append(OPERATOR_DIFFERENT);
            break;
          }
          text.append(SPACE);
          generateFilterValue(text, has.value);
        }

        if (has.value instanceof Collection<?>)
          text.append(PARENTHESIS_END);
      }
    }
  }

  protected void generateFilterValue(final StringBuilder text, final Object iValue) {
    if (iValue instanceof String)
      text.append(QUERY_STRING);

    final Object value;

    if (iValue instanceof Date)
      value = ((Date) iValue).getTime();
    else if (iValue != null)
      value = iValue.toString().replace("'", "\\'");
    else
      value = iValue;

    text.append(value);

    if (iValue instanceof String)
      text.append(QUERY_STRING);
  }
}
