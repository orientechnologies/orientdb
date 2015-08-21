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
package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This operator add an entry in a map. The entry is composed by a key and a value.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDocument extends OSQLFunctionMultiValueAbstract<ODocument> {
  public static final String NAME = "document";

  public OSQLFunctionDocument() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings("unchecked")
  public Object execute(Object iThis, final OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {

    if (iParams.length > 2)
      // IN LINE MODE
      context = new ODocument();

    if (iParams.length == 1) {
      if (iParams[0] instanceof ODocument)
        // INSERT EVERY DOCUMENT FIELD
        context.merge((ODocument) iParams[0], true, false);
      else if (iParams[0] instanceof Map<?, ?>)
        // INSERT EVERY SINGLE COLLECTION ITEM
        context.fields((Map<String, Object>) iParams[0]);
      else
        throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    } else if (iParams.length % 2 != 0)
      throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    else
      for (int i = 0; i < iParams.length; i += 2) {
        final String key = iParams[i].toString();
        final Object value = iParams[i + 1];

        if (value != null) {
          if (iParams.length <= 2 && context == null)
            // AGGREGATION MODE (STATEFULL)
            context = new ODocument();

          context.field(key, value);
        }
      }

    return prepareResult(context);
  }

  public String getSyntax() {
    return "document(<map>|[<key>,<value>]*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return configuredParameters.length <= 2;
  }

  @Override
  public ODocument getResult() {
    final ODocument res = context;
    context = null;
    return prepareResult(res);
  }

  protected ODocument prepareResult(ODocument res) {
    if (returnDistributedResult()) {
      final ODocument doc = new ODocument();
      doc.field("node", getDistributedStorageId());
      doc.field("context", res);
      return doc;
    } else {
      return res;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    if (returnDistributedResult()) {
      final Map<String, Map<Object, Object>> chunks = new HashMap<String, Map<Object, Object>>();
      for (Object iParameter : resultsToMerge) {
        final Map<String, Object> container = (Map<String, Object>) ((Map<Object, Object>) iParameter).get("doc");
        chunks.put((String) container.get("node"), (Map<Object, Object>) container.get("context"));
      }
      final Map<Object, Object> result = new HashMap<Object, Object>();
      for (Map<Object, Object> chunk : chunks.values()) {
        result.putAll(chunk);
      }
      return result;
    }

    return resultsToMerge.get(0);
  }
}
