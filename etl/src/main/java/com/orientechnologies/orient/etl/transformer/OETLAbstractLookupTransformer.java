/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Merges two records. Useful when a record needs to be updated rather than created. */
public abstract class OETLAbstractLookupTransformer extends OETLAbstractTransformer {
  protected String joinFieldName;
  protected Object joinValue;
  protected String lookup;
  protected ACTION unresolvedLinkAction = ACTION.NOTHING;
  private OSQLQuery<ODocument> sqlQuery;
  private OIndex index;

  @Override
  public void configure(final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);

    joinFieldName = iConfiguration.field("joinFieldName");

    if (iConfiguration.containsField("joinValue")) joinValue = iConfiguration.field("joinValue");

    if (iConfiguration.containsField("lookup")) lookup = iConfiguration.field("lookup");

    if (iConfiguration.containsField("unresolvedLinkAction"))
      unresolvedLinkAction =
          ACTION.valueOf(
              iConfiguration.field("unresolvedLinkAction").toString().toUpperCase(Locale.ENGLISH));
  }

  protected Object lookup(
      ODatabaseDocumentInternal db, Object joinValue, final boolean iReturnRIDS) {
    Object result = null;

    if (joinValue != null) {
      if (sqlQuery == null && index == null) {
        // ONLY THE FIRST TIME
        if (lookup.toUpperCase(Locale.ENGLISH).startsWith("SELECT"))
          sqlQuery = new OSQLSynchQuery<>(lookup);
        else {
          index = db.getMetadata().getIndexManagerInternal().getIndex(db, lookup);
          if (index == null) {
            getContext()
                .getMessageHandler()
                .warn(this, "WARNING: index %s not found. Lookups could be really slow", lookup);
            final String[] parts = lookup.split("\\.");
            sqlQuery =
                new OSQLSynchQuery<ODocument>(
                    "SELECT FROM " + parts[0] + " WHERE " + parts[1] + " = ?");
          }
        }
      }

      if (index != null) {
        final OType idxFieldType = index.getDefinition().getTypes()[0];
        joinValue = OType.convert(joinValue, idxFieldType.getDefaultJavaType());
        //noinspection resource
        if (index.getInternal() != null) {
          result = index.getInternal().getRids(joinValue);
        } else {
          result = index.get(joinValue);
        }
      } else {
        if (sqlQuery instanceof OSQLSynchQuery) ((OSQLSynchQuery) sqlQuery).resetPagination();

        result = db.query(sqlQuery, joinValue);
      }

      if (result instanceof Stream) {
        @SuppressWarnings("unchecked")
        final Stream<ORID> stream = (Stream<ORID>) result;
        final List<ORID> rids = stream.collect(Collectors.toList());
        if (rids.isEmpty()) {
          return null;
        }
        return rids;
      }
      if (result != null && result instanceof Collection) {
        final Collection coll = (Collection) result;

        if (!coll.isEmpty()) {
          if (iReturnRIDS) {
            // CONVERT COLLECTION OF RECORDS IN RIDS
            final List<ORID> resultRIDs = new ArrayList<ORID>(coll.size());
            for (Object o : coll) {
              if (o instanceof OIdentifiable) resultRIDs.add(((OIdentifiable) o).getIdentity());
            }
            result = resultRIDs;
          }
        } else result = null;
      } else if (result instanceof OIdentifiable) {
        if (iReturnRIDS) result = ((OIdentifiable) result).getIdentity();
        else result = ((OIdentifiable) result).getRecord();
      }
    }

    return result;
  }

  protected enum ACTION {
    NOTHING,
    WARNING,
    ERROR,
    HALT,
    SKIP,
    CREATE
  }
}
