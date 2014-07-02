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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.orientechnologies.orient.etl.transformer.OAbstractTransformer;

import java.util.Collection;

/**
 * Merges two records. Useful when a record needs to be updated rather than created.
 */
public class OMergeTransformer extends OAbstractTransformer {
  protected String               joinFieldName;
  protected String               lookup;
  protected ACTION               unresolvedLinkAction = ACTION.NOTHING;
  protected OSQLQuery<ODocument> sqlQuery;
  protected OIndex<?>            index;
  protected ODatabaseDocumentTx  db;

  protected enum ACTION {
    NOTHING, WARNING, ERROR, HALT, SKIP
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON("{parameters:[{joinFieldName:{optional:false,description:'field name containing the value to join'}},"
            + "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
            + "{unresolvedLinkAction:{optional:true,description:'action when a unresolved link is found',values:"
            + stringArray2Json(ACTION.values()) + "}}]," + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    joinFieldName = iConfiguration.field("joinFieldName");

    if (iConfiguration.containsField("lookup"))
      lookup = iConfiguration.field("lookup");

    if (iConfiguration.containsField("unresolvedLinkAction"))
      unresolvedLinkAction = ACTION.valueOf(iConfiguration.field("unresolvedLinkAction").toString().toUpperCase());

    db = processor.getDocumentDatabase();
  }

  @Override
  public String getName() {
    return "merge";
  }

  @Override
  public Object executeTransform(final Object input) {
    Object joinValue = ((ODocument) input).field(joinFieldName);
    if (joinValue != null) {

      Object result = null;

      if (sqlQuery == null && index == null) {
        // ONLY THE FIRST TIME
        if (lookup.toUpperCase().startsWith("SELECT"))
          sqlQuery = new OSQLSynchQuery<ODocument>(lookup);
        else
          index = db.getMetadata().getIndexManager().getIndex(lookup);
      }

      if (sqlQuery != null)
        result = db.query(sqlQuery, joinValue);
      else {
        final OType idxFieldType = index.getDefinition().getTypes()[0];
        joinValue = idxFieldType.convert(joinValue, idxFieldType.getDefaultJavaType());
        result = index.get(joinValue);
      }

      if (result != null)
        if (result instanceof Collection) {
          if (!((Collection) result).isEmpty())
            result = ((Collection<OIdentifiable>) result).iterator().next().getRecord();
        } else if (result instanceof OIdentifiable)
          result = ((OIdentifiable) result).getRecord();

      if (result == null) {
        // APPLY THE STRATEGY DEFINED IN unresolvedLinkAction
        switch (unresolvedLinkAction) {
        case NOTHING:
          break;
        case ERROR:
          processor.getStats().incrementErrors();
          processor.out(true, "%s: ERROR Cannot resolve join for value '%s'", getName(), joinValue);
          break;
        case WARNING:
          processor.getStats().incrementWarnings();
          processor.out(true, "%s: WARN Cannot resolve join for value '%s'", getName(), joinValue);
          break;
        case SKIP:
          return null;
        case HALT:
          throw new OETLProcessHaltedException("Cannot resolve join for value '" + joinValue + "'");
        }
      } else {
        final ODocument loadedDocument = (ODocument) result;
        ((ODocument) result).merge((ODocument) input, true, false);
        return result;
      }
    }

    return input;
  }
}
