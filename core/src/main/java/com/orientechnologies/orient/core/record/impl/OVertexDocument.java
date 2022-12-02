package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class OVertexDocument extends ODocument implements OVertex {

  private static final String CONNECTION_OUT_PREFIX = "out_";
  private static final String CONNECTION_IN_PREFIX = "in_";

  public OVertexDocument(OClass cl) {
    super(cl);
    if (!getImmutableSchemaClass().isVertexType()) {
      throw new IllegalArgumentException("" + getClassName() + " is not a vertex class");
    }
  }

  public OVertexDocument() {
    super();
  }

  public OVertexDocument(ODatabaseSession session) {
    super(session);
  }

  public OVertexDocument(ODatabaseSession session, String klass) {
    super(session, klass);
    if (!getImmutableSchemaClass().isVertexType()) {
      throw new IllegalArgumentException("" + getClassName() + " is not a vertex class");
    }
  }

  public OVertexDocument(String klass) {
    super(klass);
    if (!getImmutableSchemaClass().isVertexType()) {
      throw new IllegalArgumentException("" + getClassName() + " is not a vertex class");
    }
  }

  @Override
  public Iterable<OEdge> getEdges(ODirection direction) {
    Set<String> prefixes = new HashSet<>();
    switch (direction) {
      case BOTH:
        prefixes.add("in_");
      case OUT:
        prefixes.add("out_");
        break;
      case IN:
        prefixes.add("in_");
    }

    Set<String> candidateClasses = new HashSet<>();
    Iterator<String> fieldNames = calculatePropertyNames().iterator();
    while (fieldNames.hasNext()) {
      String fieldName = fieldNames.next();
      prefixes.stream()
          .filter(prefix -> fieldName.startsWith(prefix))
          .forEach(
              prefix -> {
                if (fieldName.equals(prefix)) {
                  candidateClasses.add("E");
                } else {
                  candidateClasses.add(fieldName.substring(prefix.length()));
                }
              });
    }
    return getEdges(direction, candidateClasses.toArray(new String[] {}));
  }

  @Override
  public Iterable<OEdge> getEdges(ODirection direction, String... labels) {
    final OMultiCollectionIterator<OEdge> iterable =
        new OMultiCollectionIterator<OEdge>().setEmbedded(true);

    labels = resolveAliases(labels);
    Iterator<String> fieldNames = null;
    if (labels != null && labels.length > 0) {
      // EDGE LABELS: CREATE FIELD NAME TABLE (FASTER THAN EXTRACT FIELD NAMES FROM THE DOCUMENT)
      Set<String> toLoadFieldNames = getEdgeFieldNames(direction, labels);

      if (toLoadFieldNames != null) {
        // EARLY FETCH ALL THE FIELDS THAT MATTERS
        deserializeFields(toLoadFieldNames.toArray(new String[] {}));
        fieldNames = toLoadFieldNames.iterator();
      }
    }

    if (fieldNames == null) fieldNames = calculatePropertyNames().iterator();

    while (fieldNames.hasNext()) {
      String fieldName = fieldNames.next();
      final OPair<ODirection, String> connection = getConnection(direction, fieldName, labels);
      if (connection == null)
        // SKIP THIS FIELD
        continue;

      Object fieldValue = getProperty(fieldName);

      if (fieldValue != null) {
        if (fieldValue instanceof OIdentifiable) {
          fieldValue = Collections.singleton(fieldValue);
        }
        if (fieldValue instanceof Collection<?>) {
          Collection<?> coll = (Collection<?>) fieldValue;

          // CREATE LAZY Iterable AGAINST COLLECTION FIELD
          if (coll instanceof ORecordLazyMultiValue) {
            iterable.add(
                new OEdgeIterator(
                    this,
                    coll,
                    ((ORecordLazyMultiValue) coll).rawIterator(),
                    connection,
                    labels,
                    coll.size()));
          } else
            iterable.add(new OEdgeIterator(this, coll, coll.iterator(), connection, labels, -1));

        } else if (fieldValue instanceof ORidBag) {
          iterable.add(
              new OEdgeIterator(
                  this,
                  fieldValue,
                  ((ORidBag) fieldValue).rawIterator(),
                  connection,
                  labels,
                  ((ORidBag) fieldValue).size()));
        }
      }
    }

    return iterable;
  }

  private String[] resolveAliases(String[] labels) {
    if (labels == null) {
      return null;
    }
    ODatabaseDocumentInternal db = getDatabaseIfDefined();
    if (db == null) {
      return labels;
    }
    OSchema schema = getDatabaseIfDefined().getMetadata().getSchema();
    String[] result = new String[labels.length];
    for (int i = 0; i < labels.length; i++) {
      OClass clazz = schema.getClass(labels[i]);
      if (clazz != null) {
        result[i] = clazz.getName();
      } else {
        result[i] = labels[i];
      }
    }
    return result;
  }

  @Override
  public Iterable<OEdge> getEdges(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }
    return getEdges(direction, types.toArray(new String[] {}));
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction) {
    return getVertices(direction, (String[]) null);
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction, String... type) {
    if (direction == ODirection.BOTH) {
      OMultiCollectionIterator<OVertex> result = new OMultiCollectionIterator<>();
      result.add(getVertices(ODirection.OUT, type));
      result.add(getVertices(ODirection.IN, type));
      return result;
    } else {
      Iterable<OEdge> edges = getEdges(direction, type);
      return new OEdgeToVertexIterable(edges, direction);
    }
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }
    return getVertices(direction, types.toArray(new String[] {}));
  }

  @Override
  public OEdge addEdge(OVertex to) {
    return addEdge(to, "E");
  }

  @Override
  public OEdge addEdge(OVertex to, String type) {
    ODatabaseDocument db = getDatabase();
    return db.newEdge(this, to, type == null ? "E" : type);
  }

  @Override
  public OEdge addEdge(OVertex to, OClass type) {
    String className = "E";
    if (type != null) {
      className = type.getName();
    }
    return addEdge(to, className);
  }

  public ORID moveTo(final String iClassName, final String iClusterName) {
    return moveTo(iClassName, iClusterName, this, getDatabase());
  }

  protected static ORID moveTo(
      final String iClassName, final String iClusterName, OVertex toMove, ODatabaseSession db) {
    if (toMove == null) {
      throw new ORecordNotFoundException(null, "The vertex is null");
    }
    if (checkDeletedInTx(toMove.getIdentity())) {
      throw new ORecordNotFoundException(
          toMove.getIdentity(), "The vertex " + toMove.getIdentity() + " has been deleted");
    }
    boolean moveTx = !db.getTransaction().isActive();
    try {
      if (moveTx) {
        db.begin();
      }

      final ORID oldIdentity = toMove.getIdentity().copy();

      final ORecord oldRecord = oldIdentity.getRecord();
      if (oldRecord == null)
        throw new ORecordNotFoundException(
            oldIdentity, "The vertex " + oldIdentity + " has been deleted");

      final ODocument doc = toMove.getRecord().copy();

      final Iterable<OEdge> outEdges = toMove.getEdges(ODirection.OUT);
      final Iterable<OEdge> inEdges = toMove.getEdges(ODirection.IN);

      // DELETE THE OLD RECORD FIRST TO AVOID ISSUES WITH UNIQUE CONSTRAINTS
      copyRidBags(oldRecord, doc); // TODO! check this!!!
      detachRidbags(oldRecord);
      db.delete(oldRecord);

      if (iClassName != null)
        // OVERWRITE CLASS
        doc.setClassName(iClassName);

      // SAVE THE NEW VERTEX
      doc.setDirty();

      // RESET IDENTITY
      ORecordInternal.setIdentity(doc, new ORecordId());

      db.save(doc, iClusterName);

      final ORID newIdentity = doc.getIdentity();

      // CONVERT OUT EDGES
      for (OEdge oe : outEdges) {
        if (oe.isLightweight()) {
          // REPLACE ALL REFS IN inVertex
          final OVertex inV = oe.getVertex(ODirection.IN);

          final String inFieldName =
              getConnectionFieldName(
                  ODirection.IN, oe.getSchemaType().map(x -> x.getName()).orElse(null), true);

          replaceLinks(inV.getRecord(), inFieldName, oldIdentity, newIdentity);
        } else {
          // REPLACE WITH NEW VERTEX
          oe.setProperty("out", newIdentity);
        }
        db.save(oe);
      }

      for (OEdge oe : inEdges) {
        if (oe.isLightweight()) {
          // REPLACE ALL REFS IN outVertex
          final OVertex outV = oe.getVertex(ODirection.OUT);

          final String outFieldName =
              getConnectionFieldName(
                  ODirection.OUT, oe.getSchemaType().map(x -> x.getName()).orElse(null), true);

          replaceLinks(outV.getRecord(), outFieldName, oldIdentity, newIdentity);
        } else {
          // REPLACE WITH NEW VERTEX
          oe.setProperty("in", newIdentity);
        }
        db.save(oe);
      }

      // FINAL SAVE
      db.save(doc);
      if (moveTx) {
        db.commit();
      }
      return newIdentity;
    } catch (RuntimeException ex) {
      if (moveTx) {
        db.rollback();
      }
      throw ex;
    }
  }

  private static void detachRidbags(ORecord oldRecord) {
    ODocument oldDoc = (ODocument) oldRecord;
    for (String field : oldDoc.fieldNames()) {
      if (field.equalsIgnoreCase("out")
          || field.equalsIgnoreCase("in")
          || field.startsWith("out_")
          || field.startsWith("in_")
          || field.startsWith("OUT_")
          || field.startsWith("IN_")) {
        Object val = oldDoc.rawField(field);
        if (val instanceof ORidBag) {
          oldDoc.removeField(field);
        }
      }
    }
  }

  @Override
  public OVertexDocument delete() {
    super.delete();
    return this;
  }

  public static void deleteLinks(OVertex delegate) {
    Iterable<OEdge> allEdges = delegate.getEdges(ODirection.BOTH);
    List<OEdge> items = new ArrayList<>();
    for (OEdge edge : allEdges) {
      items.add(edge);
    }
    for (OEdge edge : items) {
      edge.delete();
    }
  }

  public static String getConnectionFieldName(
      final ODirection iDirection,
      final String iClassName,
      final boolean useVertexFieldsForEdgeLabels) {
    if (iDirection == null || iDirection == ODirection.BOTH)
      throw new IllegalArgumentException("Direction not valid");

    if (useVertexFieldsForEdgeLabels) {
      // PREFIX "out_" or "in_" TO THE FIELD NAME
      final String prefix =
          iDirection == ODirection.OUT ? CONNECTION_OUT_PREFIX : CONNECTION_IN_PREFIX;
      if (iClassName == null || iClassName.isEmpty() || iClassName.equals("E")) return prefix;

      return prefix + iClassName;
    } else
      // "out" or "in"
      return iDirection == ODirection.OUT ? "out" : "in";
  }

  protected Set<String> getEdgeFieldNames(final ODirection iDirection, String... iClassNames) {
    if (iClassNames != null && iClassNames.length == 1 && iClassNames[0].equalsIgnoreCase("E"))
      // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
      iClassNames = null;

    final Set<String> result = new HashSet<>();

    if (iClassNames == null)
    // FALL BACK TO LOAD ALL FIELD NAMES
    {
      return null;
    }
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return null;
    }

    OSchema schema = db.getMetadata().getImmutableSchemaSnapshot();

    Set<String> allClassNames = new HashSet<String>();
    for (String className : iClassNames) {
      allClassNames.add(className);
      OClass clazz = schema.getClass(className);
      if (clazz != null) {
        allClassNames.add(clazz.getName()); // needed for aliases
        Collection<OClass> subClasses = clazz.getAllSubclasses();
        for (OClass subClass : subClasses) {
          allClassNames.add(subClass.getName());
        }
      }
    }

    for (String className : allClassNames) {
      switch (iDirection) {
        case OUT:
          result.add(CONNECTION_OUT_PREFIX + className);
          break;
        case IN:
          result.add(CONNECTION_IN_PREFIX + className);
          break;
        case BOTH:
          result.add(CONNECTION_OUT_PREFIX + className);
          result.add(CONNECTION_IN_PREFIX + className);
          break;
      }
    }

    return result;
  }

  protected static boolean checkDeletedInTx(ORID id) {

    ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
    if (db == null) return false;

    final ORecordOperation oper = db.getTransaction().getRecordEntry(id);
    if (oper == null) return id.isTemporary();
    else return oper.type == ORecordOperation.DELETED;
  }

  private static void copyRidBags(ORecord oldRecord, ODocument newDoc) {
    ODocument oldDoc = (ODocument) oldRecord;
    for (String field : oldDoc.fieldNames()) {
      if (field.equalsIgnoreCase("out")
          || field.equalsIgnoreCase("in")
          || field.startsWith("out_")
          || field.startsWith("in_")
          || field.startsWith("OUT_")
          || field.startsWith("IN_")) {
        Object val = oldDoc.rawField(field);
        if (val instanceof ORidBag) {
          ORidBag bag = (ORidBag) val;
          if (!bag.isEmbedded()) {
            ORidBag newBag = new ORidBag();
            Iterator<OIdentifiable> rawIter = bag.rawIterator();
            while (rawIter.hasNext()) {
              newBag.add(rawIter.next());
            }
            newDoc.field(field, newBag);
          }
        }
      }
    }
  }

  public static void replaceLinks(
      final ODocument iVertex,
      final String iFieldName,
      final OIdentifiable iVertexToRemove,
      final OIdentifiable iNewVertex) {
    if (iVertex == null) return;

    final Object fieldValue =
        iVertexToRemove != null ? iVertex.field(iFieldName) : iVertex.removeField(iFieldName);
    if (fieldValue == null) return;

    if (fieldValue instanceof OIdentifiable) {
      // SINGLE RECORD

      if (iVertexToRemove != null) {
        if (!fieldValue.equals(iVertexToRemove)) {
          return;
        }
        iVertex.field(iFieldName, iNewVertex);
      }

    } else if (fieldValue instanceof ORidBag) {
      // COLLECTION OF RECORDS: REMOVE THE ENTRY
      final ORidBag bag = (ORidBag) fieldValue;

      boolean found = false;
      final Iterator<OIdentifiable> it = bag.rawIterator();
      while (it.hasNext()) {
        if (it.next().equals(iVertexToRemove)) {
          // REMOVE THE OLD ENTRY
          found = true;
          it.remove();
        }
      }
      if (found)
        // ADD THE NEW ONE
        bag.add(iNewVertex);

    } else if (fieldValue instanceof Collection) {
      final Collection col = (Collection) fieldValue;

      if (col.remove(iVertexToRemove)) col.add(iNewVertex);
    }

    iVertex.save();
  }

  protected OPair<ODirection, String> getConnection(
      final ODirection iDirection, final String iFieldName, String... iClassNames) {
    if (iClassNames != null && iClassNames.length == 1 && iClassNames[0].equalsIgnoreCase("E"))
      // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
      iClassNames = null;

    OSchema schema =
        ODatabaseRecordThreadLocal.instance().get().getMetadata().getImmutableSchemaSnapshot();

    if (iDirection == ODirection.OUT || iDirection == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "out_"
      if (iFieldName.startsWith(CONNECTION_OUT_PREFIX)) {
        if (iClassNames == null || iClassNames.length == 0)
          return new OPair<ODirection, String>(
              ODirection.OUT, getConnectionClass(ODirection.OUT, iFieldName));

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : iClassNames) {

          if (iFieldName.equals(CONNECTION_OUT_PREFIX + clsName))
            return new OPair<ODirection, String>(ODirection.OUT, clsName);

          // GO DOWN THROUGH THE INHERITANCE TREE
          OClass type = schema.getClass(clsName);
          if (type != null) {
            for (OClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();

              if (iFieldName.equals(CONNECTION_OUT_PREFIX + clsName))
                return new OPair<ODirection, String>(ODirection.OUT, clsName);
            }
          }
        }
      }
    }

    if (iDirection == ODirection.IN || iDirection == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "in_"
      if (iFieldName.startsWith(CONNECTION_IN_PREFIX)) {
        if (iClassNames == null || iClassNames.length == 0)
          return new OPair<ODirection, String>(
              ODirection.IN, getConnectionClass(ODirection.IN, iFieldName));

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : iClassNames) {

          if (iFieldName.equals(CONNECTION_IN_PREFIX + clsName))
            return new OPair<ODirection, String>(ODirection.IN, clsName);

          // GO DOWN THROUGH THE INHERITANCE TREE
          OClass type = schema.getClass(clsName);
          if (type != null) {
            for (OClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();

              if (iFieldName.equals(CONNECTION_IN_PREFIX + clsName))
                return new OPair<ODirection, String>(ODirection.IN, clsName);
            }
          }
        }
      }
    }

    // NOT FOUND
    return null;
  }

  private String getConnectionClass(final ODirection iDirection, final String iFieldName) {
    if (iDirection == ODirection.OUT) {
      if (iFieldName.length() > CONNECTION_OUT_PREFIX.length())
        return iFieldName.substring(CONNECTION_OUT_PREFIX.length());
    } else if (iDirection == ODirection.IN) {
      if (iFieldName.length() > CONNECTION_IN_PREFIX.length())
        return iFieldName.substring(CONNECTION_IN_PREFIX.length());
    }
    return "E"; // TODO constant
  }

  @Override
  public OVertexDocument copy() {
    return (OVertexDocument) copyTo(new OVertexDocument());
  }
}
