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
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;

import java.util.*;

/**
 * This function implements asymmetric difference between sets.
 *
 *
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the DIFFERENCE between the collections received as parameters. Works also with no collection values.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDifference extends OSQLFunctionMultiValueAbstract<Set<Object>> {

  boolean first = true;

  static class ODocumentContent {
    ODocument doc;

    ODocumentContent(ODocument doc) {
      this.doc = doc;
    }

    @Override
    public boolean equals(Object obj) {
      if (doc.getIdentity().isPersistent() && obj instanceof ODocumentContent) {
        return doc.equals(((ODocumentContent) obj).doc);
      }
      if (obj instanceof ODocument) {
        return ODocumentHelper.hasSameContentOf(doc, null, (ODocument) obj, null, null);
      }
      if (obj instanceof ODocumentContent) {
        return ODocumentHelper.hasSameContentOf(doc, null, ((ODocumentContent) obj).doc, null, null, false);
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = 0;
      for (Object o : doc.fieldValues()) {
        if (o != null) {
          result += o.hashCode();
        }
      }
      return result;
    }
  }

  public static final String NAME = "difference";

  public OSQLFunctionDifference() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings("unchecked")
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {
    if (iParams[0] == null)
      return null;

    if (context == null) {
      context = new HashSet<Object>();
    }
    if (iParams.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      Object value = iParams[0];

      if (value instanceof Collection<?>) {

        for (Object o : (Collection) value) {
          if (first) {
            if (o instanceof ODocument) {
              context.add(o);
            } else if (o instanceof Collection) {
              context.addAll((Collection) o);
            } else {
              context.add(o);
            }
            first = false;
          } else {
            context = getDifferenceOf(context, o);
          }
        }
      }

      return null;
    } else {
      // IN-LINE MODE (STATELESS)

      boolean first = true;
      for (Object o : iParams) {
        if (first) {
          if (o instanceof ODocument) {
            context.add(o);
          } else if (o instanceof Collection) {
            context.addAll((Collection) o);
          } else {
            context.add(o);
          }
          first = false;
        } else {
          context = getDifferenceOf(context, o);
        }
      }
      return context;
    }
  }

  public Set<Object> getDifferenceOf(Object first, Object second) {
    final Set<Object> result = new HashSet<Object>();
    final Set<Object> boxedResult = new HashSet<Object>();
    if (first instanceof ODocument) {
      boxedResult.add(new OSQLFunctionDifference.ODocumentContent((ODocument) first));
    } else if (first instanceof Iterable) {
      for (Object o : (Iterable) first) {
        if (o instanceof ODocument) {
          boxedResult.add(new OSQLFunctionDifference.ODocumentContent((ODocument) o));
        } else {
          boxedResult.add(o);
        }
      }
    } else {
      boxedResult.add(first);
    }

    Object next = second;
    if (next instanceof ODocument) {
      boxedResult.remove(new OSQLFunctionDifference.ODocumentContent((ODocument) next));
    } else if (next instanceof Iterable) {
      for (Object o : (Iterable) next) {
        if (o instanceof ODocument) {
          boxedResult.remove(new OSQLFunctionDifference.ODocumentContent((ODocument) o));
        } else {
          boxedResult.remove(o);
        }
      }
    } else {
      boxedResult.remove(next);
    }

    for (Object o : boxedResult) {
      if (o instanceof OSQLFunctionDifference.ODocumentContent) {
        result.add(((OSQLFunctionDifference.ODocumentContent) o).doc);
      } else {
        result.add(o);
      }
    }
    return result;
  }

  @Override
  public Set<Object> getResult() {
    if (returnDistributedResult()) {
      final Map<String, Object> doc = new HashMap<String, Object>();
      doc.put("result", context);
      return Collections.<Object> singleton(doc);
    } else {
      return super.getResult();
    }
  }

  public String getSyntax() {
    return "difference(<field>*)";
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    Set<Object> result = new HashSet<Object>();

    boolean first = true;
    for (Object item : resultsToMerge) {
      if (first) {
        if (item instanceof Collection) {
          result.addAll((Collection<?>) item);
        } else {
          result.add(item);
        }
        first = false;
      } else {
        result = getDifferenceOf(result, item);
      }
    }
    return result;
  }

}
