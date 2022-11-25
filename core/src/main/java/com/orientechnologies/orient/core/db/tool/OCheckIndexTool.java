/*
 *
 *  *  Copyright 2017 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/** @author Luigi Dell'Aquila (l.dellaquila -at- orientdb.com) */
public class OCheckIndexTool extends ODatabaseTool {

  //  class Error {
  //
  //    ORID    rid;
  //    String  indexName;
  //    boolean presentInIndex;
  //    boolean presentOnCluster;
  //
  //    Error(ORID rid, String indexName, boolean presentInIndex, boolean presentOnCluster) {
  //      this.rid = rid;
  //      this.indexName = indexName;
  //      this.presentInIndex = presentInIndex;
  //      this.presentOnCluster = presentOnCluster;
  //    }
  //  }
  //
  //  List<Error> errors = new ArrayList<Error>();
  private long totalErrors = 0;

  @Override
  protected void parseSetting(String option, List<String> items) {}

  @Override
  public void run() {
    for (OIndex index : database.getMetadata().getIndexManagerInternal().getIndexes(database)) {
      if (!canCheck(index)) {
        continue;
      }
      checkIndex(index);
    }
    message("Total errors found on indexes: " + getTotalErrors());
  }

  private boolean canCheck(OIndex index) {
    OIndexDefinition indexDef = index.getDefinition();
    String className = indexDef.getClassName();
    if (className == null) {
      return false; // manual index, not supported yet
    }
    List<String> fields = indexDef.getFields();
    List<String> fieldDefs = indexDef.getFieldsToIndex();

    // check if there are fields defined on maps (by key/value). Not supported yet
    for (int i = 0; i < fieldDefs.size(); i++) {
      if (!fields.get(i).equals(fieldDefs.get(i))) {
        return false;
      }
    }
    return true;
  }

  private void checkIndex(OIndex index) {
    List<String> fields = index.getDefinition().getFields();
    String className = index.getDefinition().getClassName();
    OClass clazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(className);
    int[] clusterIds = clazz.getPolymorphicClusterIds();
    for (int clusterId : clusterIds) {
      checkCluster(clusterId, index, fields);
    }
  }

  private void checkCluster(int clusterId, OIndex index, List<String> fields) {
    long totRecordsForCluster = database.countClusterElements(clusterId);
    String clusterName = database.getClusterNameById(clusterId);

    int totSteps = 5;
    message("Checking cluster " + clusterName + "  for index " + index.getName() + "\n");
    ORecordIteratorCluster<ORecord> iter = database.browseCluster(clusterName);
    long count = 0;
    long step = -1;
    while (iter.hasNext()) {
      long currentStep = count * totSteps / totRecordsForCluster;
      if (currentStep > step) {
        printProgress(clusterName, clusterId, (int) currentStep, totSteps);
        step = currentStep;
      }
      ORecord record = iter.next();
      if (record instanceof ODocument) {
        ODocument doc = (ODocument) record;
        checkThatRecordIsIndexed(doc, index, fields);
      }
      count++;
    }
    printProgress(clusterName, clusterId, totSteps, totSteps);
    message("\n");
  }

  void printProgress(String clusterName, int clusterId, int step, int totSteps) {
    StringBuilder msg = new StringBuilder();
    msg.append("\rcluster " + clusterName + " (" + clusterId + ") |");
    for (int i = 0; i < totSteps; i++) {
      if (i < step) {
        msg.append("*");
      } else {
        msg.append(" ");
      }
    }
    msg.append("| ");
    msg.append(step * 100 / totSteps);
    msg.append("%%");
    message(msg.toString());
  }

  private void checkThatRecordIsIndexed(ODocument doc, OIndex index, List<String> fields) {
    Object[] vals = new Object[fields.size()];
    ORID docId = doc.getIdentity();
    for (int i = 0; i < vals.length; i++) {
      vals[i] = doc.field(fields.get(i));
    }

    Object indexKey = index.getDefinition().createValue(vals);
    if (indexKey == null) {
      return;
    }

    final Collection<Object> indexKeys;
    if (!(indexKey instanceof Collection)) {
      indexKeys = Collections.singletonList(indexKey);
    } else {
      //noinspection unchecked
      indexKeys = (Collection<Object>) indexKey;
    }

    for (final Object key : indexKeys) {
      try (final Stream<ORID> stream = index.getInternal().getRids(key)) {
        if (stream.noneMatch((rid) -> rid.equals(docId))) {
          totalErrors++;
          message(
              "\rERROR: Index "
                  + index.getName()
                  + " - record not found: "
                  + doc.getIdentity()
                  + "\n");
        }
      }
    }
  }

  public long getTotalErrors() {
    return totalErrors;
  }
}
