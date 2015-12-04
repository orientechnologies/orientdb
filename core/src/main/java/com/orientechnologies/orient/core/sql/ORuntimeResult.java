/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemAbstract;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import com.orientechnologies.orient.core.sql.methods.OSQLMethodRuntime;

import java.util.*;
import java.util.Map.Entry;

/**
 * Handles runtime results.
 *
 * @author Luca Garulli
 */
public class ORuntimeResult {
  private final Object              fieldValue;
  private final Map<String, Object> projections;
  private final ODocument           value;
  private OCommandContext           context;

  public ORuntimeResult(final Object iFieldValue, final Map<String, Object> iProjections, final int iProgressive,
      final OCommandContext iContext) {
    fieldValue = iFieldValue;
    projections = iProjections;
    context = iContext;
    value = createProjectionDocument(iProgressive);
  }

  public static ODocument createProjectionDocument(final int iProgressive) {
    final ODocument doc = new ODocument().setOrdered(true).setTrackingChanges(false);
    // ASSIGN A TEMPORARY RID TO ALLOW PAGINATION IF ANY
    ((ORecordId) doc.getIdentity()).clusterId = -2;
    ((ORecordId) doc.getIdentity()).clusterPosition = iProgressive;
    return doc;
  }

  @SuppressWarnings("unchecked")
  public static ODocument applyRecord(final ODocument iValue, final Map<String, Object> iProjections,
      final OCommandContext iContext, final OIdentifiable iRecord) {
    // APPLY PROJECTIONS
    final ODocument inputDocument = (ODocument) (iRecord != null ? iRecord.getRecord() : null);

    if (iProjections.isEmpty())
      // SELECT * CASE
      inputDocument.copyTo(iValue);
    else {

      for (Entry<String, Object> projection : iProjections.entrySet()) {
        final String prjName = projection.getKey();
        final Object v = projection.getValue();

        if (v == null && prjName != null) {
          iValue.field(prjName, (Object) null);
          continue;
        }

        final Object projectionValue;
        if (v.equals("*")) {
          // COPY ALL
          inputDocument.copyTo(iValue);
          // CONTINUE WITH NEXT ITEM
          continue;

        } else if (v instanceof OSQLFilterItemVariable || v instanceof OSQLFilterItemField) {
          final OSQLFilterItemAbstract var = (OSQLFilterItemAbstract) v;
          final OPair<OSQLMethodRuntime, Object[]> last = var.getLastChainOperator();
          if (last != null && last.getKey().getMethod() instanceof OSQLMethodField && last.getValue() != null
              && last.getValue().length == 1 && last.getValue()[0].equals("*")) {
            final Object value = ((OSQLFilterItemAbstract) v).getValue(inputDocument, iValue, iContext);
            if (inputDocument != null && value != null && inputDocument instanceof ODocument && value instanceof ODocument) {
              // COPY FIELDS WITH PROJECTION NAME AS PREFIX
              for (String fieldName : ((ODocument) value).fieldNames()) {
                iValue.field(prjName + fieldName, ((ODocument) value).field(fieldName));
              }
            }
            projectionValue = null;
          } else
            // RETURN A VARIABLE FROM THE CONTEXT
            projectionValue = ((OSQLFilterItemAbstract) v).getValue(inputDocument, iValue, iContext);

        } else if (v instanceof OSQLFunctionRuntime) {
          final OSQLFunctionRuntime f = (OSQLFunctionRuntime) v;
          projectionValue = f.execute(inputDocument, inputDocument, iValue, iContext);
        } else {
          if (v == null) {
            // SIMPLE NULL VALUE: SET IT IN DOCUMENT
            iValue.field(prjName, v);
            continue;
          }
          projectionValue = v;
        }

        if (projectionValue != null)
          if (projectionValue instanceof ORidBag)
            iValue.field(prjName, new ORidBag((ORidBag) projectionValue));
          else if (projectionValue instanceof OIdentifiable && !(projectionValue instanceof ORID)
              && !(projectionValue instanceof ORecord))
            iValue.field(prjName, ((OIdentifiable) projectionValue).getRecord());
          else if (projectionValue instanceof Iterator) {
            boolean link = true;
            // make temporary value typical case graph database elemenet's iterator edges
            if (projectionValue instanceof OResettable)
              ((OResettable) projectionValue).reset();

            final List<Object> iteratorValues = new ArrayList<Object>();
            final Iterator projectionValueIterator = (Iterator) projectionValue;
            while (projectionValueIterator.hasNext()) {
              Object value = projectionValueIterator.next();
              if (value instanceof OIdentifiable) {
                value = ((OIdentifiable) value).getRecord();
                if (!((OIdentifiable) value).getIdentity().isPersistent())
                  link = false;
              }

              if (value != null)
                iteratorValues.add(value);
            }

            iValue.field(prjName, iteratorValues, link ? OType.LINKLIST : OType.EMBEDDEDLIST);
          } else if (projectionValue instanceof ODocument && !((ODocument) projectionValue).getIdentity().isPersistent()) {
            iValue.field(prjName, projectionValue, OType.EMBEDDED);
          } else if (projectionValue instanceof Set<?>) {
            OType type = OType.getTypeByValue(projectionValue);
            if (type == OType.LINKSET && !entriesPersistent((Collection<OIdentifiable>) projectionValue))
              type = OType.EMBEDDEDSET;
            iValue.field(prjName, projectionValue, type);
          } else if (projectionValue instanceof Map<?, ?>) {
            OType type = OType.getTypeByValue(projectionValue);
            if (type == OType.LINKMAP && !entriesPersistent(((Map<?, OIdentifiable>) projectionValue).values()))
              type = OType.EMBEDDEDMAP;
            iValue.field(prjName, projectionValue, type);
          } else if (projectionValue instanceof List<?>) {
            OType type = OType.getTypeByValue(projectionValue);
            if (type == OType.LINKLIST && !entriesPersistent((Collection<OIdentifiable>) projectionValue))
              type = OType.EMBEDDEDLIST;
            iValue.field(prjName, projectionValue, type);

          } else
            iValue.field(prjName, projectionValue);

      }
    }

    return iValue;
  }

  private static boolean entriesPersistent(Collection<OIdentifiable> projectionValue) {
    for (OIdentifiable rec : projectionValue) {
      if (rec != null && !rec.getIdentity().isPersistent())
        return false;
    }
    return true;
  }

  public static ODocument getResult(final ODocument iValue, final Map<String, Object> iProjections) {
    if (iValue != null) {

      boolean canExcludeResult = false;

      for (Entry<String, Object> projection : iProjections.entrySet()) {
        if (!iValue.containsField(projection.getKey())) {
          // ONLY IF NOT ALREADY CONTAINS A VALUE, OTHERWISE HAS BEEN SET MANUALLY (INDEX?)
          final Object v = projection.getValue();
          if (v instanceof OSQLFunctionRuntime) {
            final OSQLFunctionRuntime f = (OSQLFunctionRuntime) v;
            canExcludeResult = f.filterResult();

            Object fieldValue = f.getResult();

            if (fieldValue != null)
              iValue.field(projection.getKey(), fieldValue);
          }
        }
      }

      if (canExcludeResult && iValue.isEmpty())
        // RESULT EXCLUDED FOR EMPTY RECORD
        return null;

      // AVOID SAVING OF TEMP RECORD
      ORecordInternal.unsetDirty(iValue);
    }
    return iValue;
  }

  public static ODocument getProjectionResult(final int iId, final Map<String, Object> iProjections,
      final OCommandContext iContext, final OIdentifiable iRecord) {
    return ORuntimeResult.getResult(
        ORuntimeResult.applyRecord(ORuntimeResult.createProjectionDocument(iId), iProjections, iContext, iRecord), iProjections);
  }

  public void applyRecord(final OIdentifiable iRecord) {
    applyRecord(value, projections, context, iRecord);
  }

  /**
   * Set a single value. This is useful in case of query optimization like with indexes
   *
   * @param iName
   *          Field name
   * @param iValue
   *          Field value
   */
  public void applyValue(final String iName, final Object iValue) {
    value.field(iName, iValue);
  }

  public ODocument getResult() {
    return getResult(value, projections);
  }

  public Object getFieldValue() {
    return fieldValue;
  }
}
