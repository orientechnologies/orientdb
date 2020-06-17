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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import java.util.logging.Level;

/** Merges two records. Useful when a record needs to be updated rather than created. */
public class OETLMergeTransformer extends OETLAbstractLookupTransformer {
  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON(
            "{parameters:["
                + getCommonConfigurationParameters()
                + ","
                + "{joinFieldName:{optional:false,description:'field name containing the value to join'}},"
                + "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
                + "{unresolvedLinkAction:{optional:true,description:'action when a unresolved link is found',values:"
                + stringArray2Json(ACTION.values())
                + "}}],"
                + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public String getName() {
    return "merge";
  }

  @Override
  public Object executeTransform(ODatabaseDocument db, final Object input) {
    Object joinValue = ((ODocument) ((OIdentifiable) input).getRecord()).field(joinFieldName);
    final Object result = lookup((ODatabaseDocumentInternal) db, joinValue, false);

    log(Level.FINE, "%s: joinValue=%s, lookupResult=%s", getName(), joinValue, result);

    if (result != null) {
      if (result instanceof OIdentifiable) {
        ((ODocument) result).merge((ODocument) input, true, false);
        log(Level.FINE, "%s: merged record %s with found record=%s", getName(), result, input);
        return result;

      } else if (OMultiValue.isMultiValue(result) && OMultiValue.getSize(result) == 1) {
        final ODocument firstValue =
            (ODocument) ((OIdentifiable) OMultiValue.getFirstValue(result)).getRecord();
        firstValue.merge((ODocument) ((OIdentifiable) input).getRecord(), true, false);
        log(Level.FINE, "%s: merged record %s with found record=%s", getName(), firstValue, input);
        return firstValue;
      } else if (OMultiValue.isMultiValue(result) && OMultiValue.getSize(result) > 1) {
        throw new OETLProcessHaltedException(
            "[Merge transformer] Multiple results returned from join for value '"
                + joinValue
                + "'");
      }
    } else {

      // APPLY THE STRATEGY DEFINED IN unresolvedLinkAction
      switch (unresolvedLinkAction) {
        case NOTHING:
          log(
              Level.FINE,
              "%s: DOING NOTHING for unresolved link on value %s",
              getName(),
              joinValue);
          break;
        case ERROR:
          processor.getStats().incrementErrors();
          log(Level.SEVERE, "%s: ERROR Cannot resolve join for value '%s'", getName(), joinValue);
          break;
        case WARNING:
          processor.getStats().incrementWarnings();
          log(Level.INFO, "%s: WARN Cannot resolve join for value '%s'", getName(), joinValue);
          break;
        case SKIP:
          log(Level.FINE, "%s: SKIPPING unresolved link on value %s", getName(), joinValue);
          return null;
        case HALT:
          throw new OETLProcessHaltedException(
              "[Merge transformer] Cannot resolve join for value '" + joinValue + "'");
      }
    }

    return input;
  }
}
