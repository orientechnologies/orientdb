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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import java.text.Collator;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Comparator implementation class used by ODocumentSorter class to sort documents following dynamic
 * criteria.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODocumentComparator implements Comparator<OIdentifiable> {
  private List<OPair<String, String>> orderCriteria;
  private OCommandContext context;
  private Collator collator;

  public ODocumentComparator(
      final List<OPair<String, String>> iOrderCriteria, OCommandContext iContext) {
    this.orderCriteria = iOrderCriteria;
    this.context = iContext;
    ODatabaseDocumentInternal internal = ODatabaseRecordThreadLocal.instance().get();
    collator =
        Collator.getInstance(
            new Locale(
                internal.get(ATTRIBUTES.LOCALECOUNTRY)
                    + "_"
                    + internal.get(ATTRIBUTES.LOCALELANGUAGE)));
  }

  @SuppressWarnings("unchecked")
  public int compare(final OIdentifiable iDoc1, final OIdentifiable iDoc2) {
    if (iDoc1 != null && iDoc1.equals(iDoc2)) return 0;

    Object fieldValue1;
    Object fieldValue2;

    int partialResult = 0;

    for (OPair<String, String> field : orderCriteria) {
      final String fieldName = field.getKey();
      final String ordering = field.getValue();

      fieldValue1 = ((ODocument) iDoc1.getRecord()).field(fieldName);
      fieldValue2 = ((ODocument) iDoc2.getRecord()).field(fieldName);

      if (fieldValue1 == null && fieldValue2 == null) {
        continue;
      }

      if (fieldValue1 == null) return factor(-1, ordering);

      if (fieldValue2 == null) return factor(1, ordering);

      if (!(fieldValue1 instanceof Comparable<?>)) {
        context.incrementVariable(OBasicCommandContext.INVALID_COMPARE_COUNT);
        partialResult = ("" + fieldValue1).compareTo("" + fieldValue2);
      } else {
        try {
          if (collator != null && fieldValue1 instanceof String && fieldValue2 instanceof String)
            partialResult = collator.compare(fieldValue1, fieldValue2);
          else partialResult = ((Comparable<Object>) fieldValue1).compareTo(fieldValue2);
        } catch (Exception ignore) {
          context.incrementVariable(OBasicCommandContext.INVALID_COMPARE_COUNT);
          partialResult = collator.compare("" + fieldValue1, "" + fieldValue2);
        }
      }
      partialResult = factor(partialResult, ordering);

      if (partialResult != 0) break;

      // CONTINUE WITH THE NEXT FIELD
    }

    return partialResult;
  }

  private int factor(final int partialResult, final String iOrdering) {
    if (iOrdering.equals(OCommandExecutorSQLSelect.KEYWORD_DESC))
      // INVERT THE ORDERING
      return partialResult * -1;

    return partialResult;
  }
}
