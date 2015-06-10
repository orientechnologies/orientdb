package com.orientechnologies.orient.core.db.record.ridbag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Find and repair broken bonsai tree removing the double linked buckets and regenerating the whole tree with data from referring
 * records.
 * 
 * 
 * @author tglman
 *
 */
public class OBonsaiTreeRepair {

  public String repairDatabaseRidbags(ODatabaseDocumentTx db) {
    StringBuilder report = new StringBuilder();
    Set<String> clusters = db.getStorage().getClusterNames();
    for (String clusterName : clusters) {
      List<DoubleReferenceItem> doubles = new ArrayList<DoubleReferenceItem>();
      Map<OBonsaiBucketPointer, Map<ORidBag, String>> map = new HashMap<OBonsaiBucketPointer, Map<ORidBag, String>>();
      // Look for RidBags with Duplicate key.
      for (ORecord rec : db.browseCluster(clusterName)) {
        try {
          if (rec instanceof ODocument) {

            final ODocument doc = (ODocument) rec;
            for (String fieldName : doc.fieldNames()) {
              final Object fieldValue = doc.rawField(fieldName);

              if (fieldValue instanceof ORidBag) {
                ORidBag ridBag = (ORidBag) fieldValue;
                if (ridBag.getPointer().isValid()) {
                  OSBTreeBonsaiLocal<OIdentifiable, Integer> tree = (OSBTreeBonsaiLocal<OIdentifiable, Integer>) db
                      .getSbTreeCollectionManager().loadSBTree(ridBag.getPointer());
                  List<OBonsaiBucketPointer> buckets = tree.listBuckets();
                  for (OBonsaiBucketPointer oBonsaiBucketPointer : buckets) {
                    Map<ORidBag, String> refBag = map.get(oBonsaiBucketPointer);
                    if (refBag != null) {
                      for (Map.Entry<ORidBag, String> entry : refBag.entrySet()) {
                        DoubleReferenceItem ref = new DoubleReferenceItem(fieldName, ridBag, entry.getValue(), entry.getKey(),
                            oBonsaiBucketPointer);
                        doubles.add(ref);
                      }
                    } else
                      refBag = new HashMap<ORidBag, String>();
                    refBag.put(ridBag, fieldName);
                    map.put(oBonsaiBucketPointer, refBag);
                  }
                }
              }

            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      Map<ODocument, Set<String>> fixedSet = new HashMap<ODocument, Set<String>>();
      for (DoubleReferenceItem doubleRef : doubles) {
        try {
          ORidBag ridBagOne = doubleRef.getRidBagOne();
          ODocument docOne = (ODocument) ridBagOne.getDelegate().getOwner();
          OSBTreeBonsaiLocal<OIdentifiable, Integer> tree = (OSBTreeBonsaiLocal<OIdentifiable, Integer>) db
              .getSbTreeCollectionManager().loadSBTree(ridBagOne.getPointer());
          tree.removeBucketsFromFreeList();
          tree.clear();

          ORidBag ridBagTwo = doubleRef.getRidBagTwo();
          OSBTreeBonsaiLocal<OIdentifiable, Integer> tree2 = (OSBTreeBonsaiLocal<OIdentifiable, Integer>) db
              .getSbTreeCollectionManager().loadSBTree(ridBagOne.getPointer());
          tree2.removeBucketsFromFreeList();
          tree2.clear();
          ODocument docTwo = (ODocument) ridBagTwo.getDelegate().getOwner();

          List<ODocument> list = regenerateRidBag(docOne, doubleRef.getFieldNameOne(), db);
          for (ODocument oDocument : list) {
            ridBagOne.add((OIdentifiable) oDocument.field("res"));
          }
          db.save(docOne);
          List<ODocument> list1 = regenerateRidBag(docTwo, doubleRef.getFieldNameTwo(), db);
          for (ODocument oDocument : list1) {
            ridBagTwo.add((OIdentifiable) oDocument.field("res"));
          }
          db.save(docTwo);

          Set<String> res = fixedSet.get(docOne);
          if (res == null) {
            res = new HashSet<String>();
            fixedSet.put(docOne, res);
          }
          res.add(doubleRef.getFieldNameOne());
          res = fixedSet.get(docTwo);
          if (res == null) {
            res = new HashSet<String>();
            fixedSet.put(docTwo, res);
          }
          res.add(doubleRef.getFieldNameTwo());
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
      if (!fixedSet.isEmpty()) {
        report.append("In cluster: " + clusterName + " fixed ridbags of records: \n");
        for (Entry<ODocument, Set<String>> entry : fixedSet.entrySet()) {
          ODocument doc = entry.getKey();
          report.append("\t").append(doc.getIdentity()).append("\n");
          for (String field : entry.getValue())
            report.append("\t\t").append(field).append("\n");
        }
      }

    }
    return report.toString();

  }

  private List<ODocument> regenerateRidBag(ODocument docOne, String fieldNameOne, ODatabaseDocumentTx db) {
    OImmutableClass clazzOne = ODocumentInternal.getImmutableSchemaClass(docOne);
    OProperty prop = clazzOne.getProperty(fieldNameOne);
    String className = null;
    if (prop != null) {
      OClass linkClass = prop.getLinkedClass();
      if (linkClass != null) {
        className = linkClass.getName();
      }
    }
    if (className == null)
      className = getEdgeLabel(fieldNameOne);
    String fieldInEdge = getEdgeField(fieldNameOne);

    String query = " select @rid as res from " + className + "  where " + fieldInEdge + " = " + docOne.getIdentity() + " ";
    List<ODocument> res = db.query(new OSQLSynchQuery<Object>(query));

    return res;
  }

  public static String getEdgeField(final String iConnectionField) {
    if (iConnectionField.startsWith("out_"))
      return "out";
    else if (iConnectionField.startsWith("in_"))
      return "in";
    return null;
  }

  public static String getEdgeLabel(final String iConnectionField) {
    if (iConnectionField.startsWith("out_"))
      return iConnectionField.substring("out_".length());
    else if (iConnectionField.startsWith("in_"))
      return iConnectionField.substring("in_".length());
    return null;
  }
}
