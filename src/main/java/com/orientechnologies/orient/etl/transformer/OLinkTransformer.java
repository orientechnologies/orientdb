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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import com.orientechnologies.orient.etl.OETLProcessor;

import java.util.*;

/**
 * Converts a JOIN in LINK
 */
public class OLinkTransformer extends OAbstractLookupTransformer {
  private String joinValue;
  private String linkFieldName;
  private OType  linkFieldType;

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON("{parameters:["
            + getCommonConfigurationParameters()
            + ","
            + "{joinFieldName:{optional:true,description:'field name containing the value to join'}},"
            + "{joinValue:{optional:true,description:'value to use in lookup query'}},"
            + "{linkFieldName:{optional:false,description:'field name containing the link to set'}},"
            + "{linkFieldType:{optional:true,description:'field type containing the link to set. Use LINK for single link and LINKSET or LINKLIST for many'}},"
            + "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
            + "{unresolvedLinkAction:{optional:true,description:'action when a unresolved link is found',values:"
            + stringArray2Json(ACTION.values()) + "}}]," + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    joinValue = iConfiguration.field("joinValue");
    linkFieldName = iConfiguration.field("linkFieldName");
    if (iConfiguration.containsField("linkFieldType"))
      linkFieldType = OType.valueOf((String) iConfiguration.field("linkFieldType"));
  }

  @Override
  public String getName() {
    return "link";
  }

  @Override
  public Object executeTransform(final Object input) {
    if (!(input instanceof OIdentifiable)) {
      log(OETLProcessor.LOG_LEVELS.DEBUG, "skip because input value is not a record, but rather an instance of class: %s", input.getClass());
      return null;
    }

    final ODocument doc = ((OIdentifiable) input).getRecord();
    final Object joinRuntimeValue;
    if (joinFieldName != null)
      joinRuntimeValue = doc.field(joinFieldName);
    else if (joinValue != null)
      joinRuntimeValue = resolve(joinValue);
    else
      joinRuntimeValue = null;

    Object result;
    if (OMultiValue.isMultiValue(joinRuntimeValue)) {
      // RESOLVE SINGLE JOINS
      final Collection<Object> singleJoinsResult = new ArrayList<Object>();
      for (Object o : OMultiValue.getMultiValueIterable(joinRuntimeValue)) {
        singleJoinsResult.add(lookup(o, true));
      }
      result = singleJoinsResult;
    } else
      result = lookup(joinRuntimeValue, true);

    log(OETLProcessor.LOG_LEVELS.DEBUG, "joinRuntimeValue=%s, lookupResult=%s", joinRuntimeValue, result);

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

      if (result == null) {
        // APPLY THE STRATEGY DEFINED IN unresolvedLinkAction
        switch (unresolvedLinkAction) {
        case CREATE:
          if (lookup != null) {
            final String[] lookupParts = lookup.split("\\.");
            final ODocument linkedDoc = new ODocument(lookupParts[0]);
            linkedDoc.field(lookupParts[1], joinRuntimeValue);
            linkedDoc.save();

            log(OETLProcessor.LOG_LEVELS.DEBUG, "created new document=%s", linkedDoc.getRecord());

            result = linkedDoc;
          } else
            throw new OConfigurationException("Cannot create linked document because target class is unknown. Use 'lookup' field");
          break;
        case ERROR:
          processor.getStats().incrementErrors();
          log(OETLProcessor.LOG_LEVELS.ERROR, "%s: ERROR Cannot resolve join for value '%s'", getName(), joinRuntimeValue);
          break;
        case WARNING:
          processor.getStats().incrementWarnings();
          log(OETLProcessor.LOG_LEVELS.INFO, "%s: WARN Cannot resolve join for value '%s'", getName(), joinRuntimeValue);
          break;
        case SKIP:
          return null;
        case HALT:
          throw new OETLProcessHaltedException("[Link transformer] Cannot resolve join for value '" + joinRuntimeValue + "'");
        }
      }
    }

    // SET THE TRANSFORMED FIELD BACK
    doc.field(linkFieldName, result);

    log(OETLProcessor.LOG_LEVELS.DEBUG, "set %s=%s in document=%s", linkFieldName, result, input);

    return input;
  }
}
