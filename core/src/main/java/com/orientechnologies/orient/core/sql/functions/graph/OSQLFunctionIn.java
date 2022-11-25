package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Created by luigidellaquila on 03/01/17. */
public class OSQLFunctionIn extends OSQLFunctionMoveFiltered {
  public static final String NAME = "in";

  public OSQLFunctionIn() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final ODatabase graph, final OIdentifiable iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, ODirection.IN, iLabels);
  }

  protected Object move(
      final ODatabase graph,
      final OIdentifiable iRecord,
      final String[] iLabels,
      Iterable<OIdentifiable> iPossibleResults) {
    if (iPossibleResults == null) {
      return v2v(graph, iRecord, ODirection.IN, iLabels);
    }

    if (!iPossibleResults.iterator().hasNext()) {
      return Collections.emptyList();
    }

    Object edges = v2e(graph, iRecord, ODirection.IN, iLabels);
    if (edges instanceof OSizeable) {
      int size = ((OSizeable) edges).size();
      if (size > supernodeThreshold) {
        Object result = fetchFromIndex(graph, iRecord, iPossibleResults, iLabels);
        if (result != null) {
          return result;
        }
      }
    }

    return v2v(graph, iRecord, ODirection.IN, iLabels);
  }

  private Object fetchFromIndex(
      ODatabase graph, OIdentifiable iFrom, Iterable<OIdentifiable> to, String[] iEdgeTypes) {
    String edgeClassName = null;
    if (iEdgeTypes == null) {
      edgeClassName = "E";
    } else if (iEdgeTypes.length == 1) {
      edgeClassName = iEdgeTypes[0];
    } else {
      return null;
    }
    OClass edgeClass =
        ((ODatabaseDocumentInternal) graph)
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClass(edgeClassName);
    if (edgeClass == null) {
      return null;
    }
    Set<OIndex> indexes = edgeClass.getInvolvedIndexes("in", "out");
    if (indexes == null || indexes.size() == 0) {
      return null;
    }
    OIndex index = indexes.iterator().next();

    OMultiCollectionIterator<OVertex> result = new OMultiCollectionIterator<OVertex>();
    for (OIdentifiable identifiable : to) {
      OCompositeKey key = new OCompositeKey(iFrom, identifiable);
      try (Stream<ORID> stream = index.getInternal().getRids(key)) {
        result.add(
            stream
                .map((edge) -> ((ODocument) edge.getRecord()).rawField("out"))
                .collect(Collectors.toSet()));
      }
    }

    return result;
  }
}
