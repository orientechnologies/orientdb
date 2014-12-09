/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.util.Collection;

/**
 * Merges two records. Useful when a record needs to be updated rather than created.
 */
public abstract class OAbstractLookupTransformer extends OAbstractTransformer {
  protected String               joinFieldName;
  protected String               lookup;
  protected ACTION               unresolvedLinkAction = ACTION.NOTHING;
  protected OSQLQuery<ODocument> sqlQuery;
  protected OIndex<?>            index;
  protected ODatabaseDocumentTx  db;

  protected enum ACTION {
    NOTHING, WARNING, ERROR, HALT, SKIP, CREATE
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    joinFieldName = iConfiguration.field("joinFieldName");

    if (iConfiguration.containsField("lookup"))
      lookup = iConfiguration.field("lookup");

    if (iConfiguration.containsField("unresolvedLinkAction"))
      unresolvedLinkAction = ACTION.valueOf(iConfiguration.field("unresolvedLinkAction").toString().toUpperCase());
  }

  @Override
  public void begin() {
    if (db == null) {
      db = pipeline.getDocumentDatabase();
      if (db == null)
        throw new OTransformException("[" + getName() + " Transformer] database is not configured");
    }
  }

  protected Object lookup(Object joinValue, final boolean iReturnRIDS) {
    Object result = null;

    if (joinValue != null) {
      if (sqlQuery == null && index == null) {
        // ONLY THE FIRST TIME
        if (lookup.toUpperCase().startsWith("SELECT"))
          sqlQuery = new OSQLSynchQuery<ODocument>(lookup);
        else {
          index = db.getMetadata().getIndexManager().getIndex(lookup);
          if (index == null) {
            log(OETLProcessor.LOG_LEVELS.DEBUG, "WARNING: index %s not found. Lookups could be really slow", lookup);
            final String[] parts = lookup.split("\\.");
            sqlQuery = new OSQLSynchQuery<ODocument>("SELECT FROM " + parts[0] + " WHERE " + parts[1] + " = ?");
          }
        }
      }

      if (index != null) {
        final OType idxFieldType = index.getDefinition().getTypes()[0];
        joinValue = idxFieldType.convert(joinValue, idxFieldType.getDefaultJavaType());
        result = index.get(joinValue);
      } else
        result = db.query(sqlQuery, joinValue);

      if (result != null)
        if (result instanceof Collection) {
          if (!((Collection) result).isEmpty())
            result = ((Collection<OIdentifiable>) result).iterator().next().getRecord();
          else
            result = null;
        } else if (result instanceof OIdentifiable)
          result = ((OIdentifiable) result).getRecord();

      if (iReturnRIDS && result instanceof ORecord)
        result = ((ORecord) result).getIdentity();

    }

    return result;
  }
}
