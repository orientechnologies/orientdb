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

package com.orientechnologies.orient.etl.transform;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Converts a JOIN in LINK
 */
public class OLinkTransformer extends OAbstractTransformer {
  protected String               joinFieldName;
  protected String               linkFieldName;
  protected OType                linkFieldType;
  protected String               lookup;
  protected ACTION               unresolvedLinkAction;
  protected OSQLQuery<ODocument> sqlQuery;
  protected OIndex<?>            index;

  protected enum ACTION {
    CREATE, WARNING, ERROR, HALT, SKIP
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON("{parameters:[{joinFieldName:{optional:false,description:'field name containing the value to join'}},{linkFieldName:{optional:false,description:'field name containing the link to set'}},"
            + "{linkFieldType:{optional:true,description:'field type containing the link to set. Use LINK for single link and LINKSET or LINKLIST for many'}},"
            + "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
            + "{unresolvedLinkAction:{optional:true,description:'action when a unresolved link is found',values:"
            + stringArray2Json(ACTION.values()) + "}}]," + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(final ODocument iConfiguration) {
    joinFieldName = iConfiguration.field("joinFieldName");
    linkFieldName = iConfiguration.field("linkFieldName");
    if (iConfiguration.containsField("linkFieldType"))
      linkFieldType = OType.valueOf((String) iConfiguration.field("linkFieldType"));

    if (iConfiguration.containsField("lookup"))
      lookup = iConfiguration.field("lookup");

    if (iConfiguration.containsField("unresolvedLinkAction"))
      unresolvedLinkAction = ACTION.valueOf(iConfiguration.field("unresolvedLinkAction").toString().toUpperCase());
  }

  @Override
  public String getName() {
    return "link";
  }

  @Override
  public Object executeTransform(final Object input, final OCommandContext iContext) {
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

      if (result != null) {
        if (linkFieldType != null) {
          // CONVERT IT
          if (linkFieldType == OType.LINK) {
            if (result instanceof Collection<?>) {
              if (!((Collection) result).isEmpty())
                result = ((Collection) result).iterator().next();
              else
                result = null;
            }
          } else if (linkFieldType == OType.LINKSET) {
            if (!(result instanceof Collection)) {
              final Set<OIdentifiable> res = new HashSet<OIdentifiable>();
              res.add((OIdentifiable) result);
              result = res;
            }
          } else if (linkFieldType == OType.LINKLIST) {
            if (!(result instanceof Collection)) {
              final List<OIdentifiable> res = new ArrayList<OIdentifiable>();
              res.add((OIdentifiable) result);
              result = res;
            }
          }
        }
      }

      if (result == null) {
        // APPLY THE STRATEGY DEFINED IN unresolvedLinkAction
        switch (unresolvedLinkAction) {
        case CREATE:
          if (lookup != null) {
            final String[] lookupParts = lookup.split("\\.");
            final ODocument linkedDoc = new ODocument(lookupParts[0]);
            linkedDoc.field(lookupParts[1], joinValue);
            linkedDoc.save();
            result = linkedDoc;
          } else
            throw new OConfigurationException("Cannot create linked document because target class is unknown. Use 'lookup' field");
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
      }

      // SET THE TRANSFORMED FIELD BACK
      ((ODocument) input).field(linkFieldName, result);
    }

    return input;
  }
}
