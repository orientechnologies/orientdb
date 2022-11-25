package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 07/09/16. */
public class FindReferencesStep extends AbstractExecutionStep {
  private final List<OIdentifier> classes;
  private final List<OCluster> clusters;

  private boolean inited = false;
  private Set<ORID> ridsToFind;
  private ORecordIteratorCluster currentIterator;
  private Iterator<ORecordIteratorCluster> clusterIterators;
  private OResultInternal nextResult;

  public FindReferencesStep(
      List<OIdentifier> classes,
      List<OCluster> clusters,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.classes = classes;
    this.clusters = clusters;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!inited) {
      init(ctx, nRecords);
    }
    return new OResultSet() {
      @Override
      public boolean hasNext() {

        return nextResult != null;
      }

      @Override
      public OResult next() {
        if (nextResult == null) {
          throw new IllegalStateException();
        }
        OResultInternal result = nextResult;
        fetchNext(ctx);
        return result;
      }

      @Override
      public void close() {}

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void init(OCommandContext ctx, int nRecords) {
    fetchRidsToFind(ctx, nRecords);
    initClusterIterators(ctx);
    fetchNext(ctx);

    this.inited = true;
  }

  private void fetchNext(OCommandContext ctx) {
    this.nextResult = null;
    while (true) {
      while (currentIterator == null || !currentIterator.hasNext()) {
        if (!clusterIterators.hasNext()) {
          return;
        }
        currentIterator = clusterIterators.next();
      }

      ORecord rec = currentIterator.next();
      if (rec instanceof ODocument) {
        OResultInternal x = new OResultInternal(rec);
        for (ORID rid : ridsToFind) {
          List<String> resultForRecord = checkObject(Collections.singleton(rid), x, rec, "");
          if (resultForRecord.size() > 0) {
            nextResult = new OResultInternal();
            nextResult.setProperty("rid", rid);
            nextResult.setProperty("referredBy", rec);
            nextResult.setProperty("fields", resultForRecord);
            return;
          }
        }
      }
    }
  }

  private void initClusterIterators(OCommandContext ctx) {
    ODatabaseInternal db = (ODatabaseInternal) ctx.getDatabase();
    Collection<String> targetClusterNames = new HashSet<>();

    if ((this.classes == null || this.classes.size() == 0)
        && (this.clusters == null || this.clusters.size() == 0)) {
      targetClusterNames.addAll(ctx.getDatabase().getClusterNames());
    } else {
      if (this.clusters != null) {
        for (OCluster c : this.clusters) {
          if (c.getClusterName() != null) {
            targetClusterNames.add(c.getClusterName());
          } else {
            String clusterName = db.getClusterNameById(c.getClusterNumber());
            if (clusterName == null) {
              throw new OCommandExecutionException("Cluster not found: " + c.getClusterNumber());
            }
            targetClusterNames.add(clusterName);
          }
        }
        OSchema schema =
            ((ODatabaseDocumentInternal) db).getMetadata().getImmutableSchemaSnapshot();
        for (OIdentifier className : this.classes) {
          OClass clazz = schema.getClass(className.getStringValue());
          if (clazz == null) {
            throw new OCommandExecutionException("Class not found: " + className);
          }
          for (int clusterId : clazz.getPolymorphicClusterIds()) {
            targetClusterNames.add(db.getClusterNameById(clusterId));
          }
        }
      }
    }

    List<ORecordIteratorCluster> iterators =
        targetClusterNames.stream()
            .map(
                clusterName ->
                    new ORecordIteratorCluster(
                        (ODatabaseDocumentInternal) db, db.getClusterIdByName(clusterName)))
            .collect(Collectors.toList());
    this.clusterIterators = iterators.iterator();
  }

  private void fetchRidsToFind(OCommandContext ctx, int nRecords) {
    ridsToFind = new HashSet<>();

    OExecutionStepInternal prevStep = getPrev().get();
    OResultSet nextSlot = prevStep.syncPull(ctx, nRecords);
    while (nextSlot.hasNext()) {
      while (nextSlot.hasNext()) {
        OResult nextRes = nextSlot.next();
        if (nextRes.isElement()) {
          ridsToFind.add(nextRes.getElement().get().getIdentity());
        }
      }
      nextSlot = prevStep.syncPull(ctx, nRecords);
    }
  }

  private static List<String> checkObject(
      final Set<ORID> iSourceRIDs, final Object value, final ORecord iRootObject, String prefix) {
    if (value instanceof OResult) {
      return checkRoot(iSourceRIDs, (OResult) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else if (value instanceof OIdentifiable) {
      return checkRecord(iSourceRIDs, (OIdentifiable) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else if (value instanceof Collection<?>) {
      return checkCollection(iSourceRIDs, (Collection<?>) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else if (value instanceof Map<?, ?>) {
      return checkMap(iSourceRIDs, (Map<?, ?>) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else {
      return new ArrayList<>();
    }
  }

  private static List<String> checkCollection(
      final Set<ORID> iSourceRIDs,
      final Collection<?> values,
      final ORecord iRootObject,
      String prefix) {
    final Iterator<?> it;
    if (values instanceof ORecordLazyMultiValue) {
      it = ((ORecordLazyMultiValue) values).rawIterator();
    } else {
      it = values.iterator();
    }
    List<String> result = new ArrayList<>();
    while (it.hasNext()) {
      result.addAll(checkObject(iSourceRIDs, it.next(), iRootObject, prefix));
    }
    return result;
  }

  private static List<String> checkMap(
      final Set<ORID> iSourceRIDs,
      final Map<?, ?> values,
      final ORecord iRootObject,
      String prefix) {
    final Iterator<?> it;
    if (values instanceof ORecordLazyMap) {
      it = ((ORecordLazyMap) values).rawIterator();
    } else {
      it = values.values().iterator();
    }
    List<String> result = new ArrayList<>();
    while (it.hasNext()) {
      result.addAll(checkObject(iSourceRIDs, it.next(), iRootObject, prefix));
    }
    return result;
  }

  private static List<String> checkRecord(
      final Set<ORID> iSourceRIDs,
      final OIdentifiable value,
      final ORecord iRootObject,
      String prefix) {
    List<String> result = new ArrayList<>();
    if (iSourceRIDs.contains(value.getIdentity())) {
      result.add(prefix);
    } else if (!value.getIdentity().isValid() && value.getRecord() instanceof ODocument) {
      // embedded document
      ODocument doc = value.getRecord();
      for (String fieldName : doc.fieldNames()) {
        Object fieldValue = doc.field(fieldName);
        result.addAll(checkObject(iSourceRIDs, fieldValue, iRootObject, prefix + "." + fieldName));
      }
    }
    return result;
  }

  private static List<String> checkRoot(
      final Set<ORID> iSourceRIDs, final OResult value, final ORecord iRootObject, String prefix) {
    List<String> result = new ArrayList<>();
    for (String fieldName : value.getPropertyNames()) {
      Object fieldValue = value.getProperty(fieldName);
      result.addAll(checkObject(iSourceRIDs, fieldValue, iRootObject, prefix + "." + fieldName));
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ FIND REFERENCES\n");
    result.append(spaces);

    if ((this.classes == null || this.classes.isEmpty())
        && (this.clusters == null || this.clusters.isEmpty())) {
      result.append("  (all db)");
    } else {
      if (this.classes != null && this.classes.size() > 0) {
        result.append("  classes: " + this.classes);
      }
      if (this.clusters != null && this.clusters.size() > 0) {
        result.append("  classes: " + this.clusters);
      }
    }
    return result.toString();
  }
}
