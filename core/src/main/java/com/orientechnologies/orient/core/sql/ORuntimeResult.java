/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql;

import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;

/**
 * Handles runtime results.
 * 
 * @author Luca Garulli
 */
public class ORuntimeResult {
  private final Map<String, Object> projections;
  private final ODocument           value;
  private OCommandContext           context;

  public ORuntimeResult(final Map<String, Object> iProjections, final int iProgressive, final OCommandContext iContext) {
    projections = iProjections;
    context = iContext;
    value = createProjectionDocument(iProgressive);
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

  public static ODocument createProjectionDocument(final int iProgressive) {
    final ODocument doc = new ODocument().setOrdered(true);
    // ASSIGN A TEMPORARY RID TO ALLOW PAGINATION IF ANY
    ((ORecordId) doc.getIdentity()).clusterId = -2;
    ((ORecordId) doc.getIdentity()).clusterPosition = iProgressive;
    return doc;
  }

  public static ODocument applyRecord(final ODocument iValue, final Map<String, Object> iProjections,
      final OCommandContext iContext, final OIdentifiable iRecord) {
    // APPLY PROJECTIONS
    final ODocument inputDocument = (ODocument) (iRecord != null ? iRecord.getRecord() : null);

    Object projectionValue;
    for (Entry<String, Object> projection : iProjections.entrySet()) {
      final Object v = projection.getValue();

      if (v.equals("*")) {
        // COPY ALL
        inputDocument.copy(iValue);
        projectionValue = null;
      } else if (v instanceof OSQLFilterItemVariable) {
        // RETURN A VARIABLE FROM THE CONTEXT
        projectionValue = ((OSQLFilterItemVariable) v).getValue(inputDocument, iContext);
      } else if (v instanceof OSQLFilterItemField)
        projectionValue = ((OSQLFilterItemField) v).getValue(inputDocument, iContext);
      else if (v instanceof OSQLFunctionRuntime) {
        final OSQLFunctionRuntime f = (OSQLFunctionRuntime) v;
        projectionValue = f.execute(inputDocument, iContext);
      } else
        projectionValue = v;

      if (projectionValue != null)
        iValue.field(projection.getKey(), projectionValue);
    }

    return iValue;
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
      iValue.unsetDirty();
    }
    return iValue;
  }

  public static ODocument getProjectionResult(final int iId, final Map<String, Object> iProjections,
      final OCommandContext iContext, final OIdentifiable iRecord) {
    return ORuntimeResult.getResult(
        ORuntimeResult.applyRecord(ORuntimeResult.createProjectionDocument(iId), iProjections, iContext, iRecord), iProjections);
  }
}
