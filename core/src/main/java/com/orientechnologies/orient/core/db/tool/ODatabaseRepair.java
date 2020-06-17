/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Iterator;
import java.util.List;

/**
 * Repair database tool.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @since v2.2.0
 */
public class ODatabaseRepair extends ODatabaseTool {
  private boolean removeBrokenLinks = true;

  @Override
  protected void parseSetting(final String option, final List<String> items) {
    if (option.equalsIgnoreCase("-excludeAll")) {

      removeBrokenLinks = false;

    } else if (option.equalsIgnoreCase("-removeBrokenLinks")) {

      removeBrokenLinks = Boolean.parseBoolean(items.get(0));
    }
  }

  public void run() {
    long errors = 0;

    if (removeBrokenLinks) errors += removeBrokenLinks();

    message("\nRepair database complete (" + errors + " errors)");
  }

  protected long removeBrokenLinks() {
    long fixedLinks = 0l;
    long modifiedDocuments = 0l;
    long errors = 0l;

    message("\n- Removing broken links...");
    for (String clusterName : database.getClusterNames()) {
      for (ORecord rec : database.browseCluster(clusterName)) {
        try {
          if (rec instanceof ODocument) {
            boolean changed = false;

            final ODocument doc = (ODocument) rec;
            for (String fieldName : doc.fieldNames()) {
              final Object fieldValue = doc.rawField(fieldName);

              if (fieldValue instanceof OIdentifiable) {
                if (fixLink(fieldValue)) {
                  doc.field(fieldName, (OIdentifiable) null);
                  fixedLinks++;
                  changed = true;
                  if (verbose)
                    message(
                        "\n--- reset link "
                            + ((OIdentifiable) fieldValue).getIdentity()
                            + " in field '"
                            + fieldName
                            + "' (rid="
                            + doc.getIdentity()
                            + ")");
                }
              } else if (fieldValue instanceof Iterable<?>) {
                if (fieldValue instanceof ORecordLazyMultiValue)
                  ((ORecordLazyMultiValue) fieldValue).setAutoConvertToRecord(false);

                final Iterator<Object> it = ((Iterable) fieldValue).iterator();
                for (int i = 0; it.hasNext(); ++i) {
                  final Object v = it.next();
                  if (fixLink(v)) {
                    it.remove();
                    fixedLinks++;
                    changed = true;
                    if (verbose)
                      message(
                          "\n--- reset link "
                              + ((OIdentifiable) v).getIdentity()
                              + " as item "
                              + i
                              + " in collection of field '"
                              + fieldName
                              + "' (rid="
                              + doc.getIdentity()
                              + ")");
                  }
                }
              }
            }

            if (changed) {
              modifiedDocuments++;
              doc.save();

              if (verbose) message("\n-- updated document " + doc.getIdentity());
            }
          }
        } catch (Exception ignore) {
          errors++;
        }
      }
    }

    message("\n-- Done! Fixed links: " + fixedLinks + ", modified documents: " + modifiedDocuments);
    return errors;
  }

  /**
   * Checks if the link must be fixed.
   *
   * @param fieldValue Field containing the OIdentifiable (RID or Record)
   * @return true to fix it, otherwise false
   */
  protected boolean fixLink(final Object fieldValue) {
    if (fieldValue instanceof OIdentifiable) {
      final ORID id = ((OIdentifiable) fieldValue).getIdentity();

      if (id.getClusterId() == 0 && id.getClusterPosition() == 0) return true;

      if (id.isValid())
        if (id.isPersistent()) {
          final ORecord connected = ((OIdentifiable) fieldValue).getRecord();
          if (connected == null) return true;
        } else return true;
    }
    return false;
  }
}
