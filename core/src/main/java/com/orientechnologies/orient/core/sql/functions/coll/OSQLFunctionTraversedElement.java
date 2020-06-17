/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.traverse.OTraverseRecordProcess;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionConfigurableAbstract;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Returns a traversed element from the stack. Use it with SQL traverse only.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionTraversedElement extends OSQLFunctionConfigurableAbstract {
  public static final String NAME = "traversedElement";

  public OSQLFunctionTraversedElement() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionTraversedElement(final String name) {
    super(name, 1, 2);
  }

  public boolean aggregateResults() {
    return false;
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  public String getSyntax() {
    return getName() + "(<beginIndex> [,<items>])";
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      final OCommandContext iContext) {
    return evaluate(iThis, iParams, iContext, null);
  }

  protected Object evaluate(
      final Object iThis,
      final Object[] iParams,
      final OCommandContext iContext,
      final String iClassName) {
    final int beginIndex = (Integer) iParams[0];
    final int items = iParams.length > 1 ? (Integer) iParams[1] : 1;

    Collection stack = (Collection) iContext.getVariable("stack");
    if (stack == null && iThis instanceof OResult) {
      stack = (Collection) ((OResult) iThis).getMetadata("$stack");
    }
    if (stack == null)
      throw new OCommandExecutionException(
          "Cannot invoke " + getName() + "() against non traverse command");

    final List<OIdentifiable> result = items > 1 ? new ArrayList<OIdentifiable>(items) : null;

    if (beginIndex < 0) {
      int i = -1;
      for (Iterator it = stack.iterator(); it.hasNext(); ) {
        final Object o = it.next();
        if (o instanceof OTraverseRecordProcess) {
          final OIdentifiable record = ((OTraverseRecordProcess) o).getTarget();

          if (iClassName == null
              || ODocumentInternal.getImmutableSchemaClass((ODocument) record.getRecord())
                  .isSubClassOf(iClassName)) {
            if (i <= beginIndex) {
              if (items == 1) return record;
              else {
                result.add(record);
                if (result.size() >= items) break;
              }
            }
            i--;
          }
        } else if (o instanceof OIdentifiable) {
          final OIdentifiable record = (OIdentifiable) o;

          if (iClassName == null
              || ODocumentInternal.getImmutableSchemaClass((ODocument) record.getRecord())
                  .isSubClassOf(iClassName)) {
            if (i <= beginIndex) {
              if (items == 1) return record;
              else {
                result.add(record);
                if (result.size() >= items) break;
              }
            }
            i--;
          }
        }
      }
    } else {
      int i = 0;
      List listStack = stackToList(stack);
      for (int x = listStack.size() - 1; x >= 0; x--) {
        final Object o = listStack.get(x);
        if (o instanceof OTraverseRecordProcess) {
          final OIdentifiable record = ((OTraverseRecordProcess) o).getTarget();

          if (iClassName == null
              || ODocumentInternal.getImmutableSchemaClass((ODocument) record.getRecord())
                  .isSubClassOf(iClassName)) {
            if (i >= beginIndex) {
              if (items == 1) return record;
              else {
                result.add(record);
                if (result.size() >= items) break;
              }
            }
            i++;
          }
        } else if (o instanceof OIdentifiable) {
          final OIdentifiable record = (OIdentifiable) o;

          if (iClassName == null
              || ODocumentInternal.getImmutableSchemaClass((ODocument) record.getRecord())
                  .isSubClassOf(iClassName)) {
            if (i >= beginIndex) {
              if (items == 1) return record;
              else {
                result.add(record);
                if (result.size() >= items) break;
              }
            }
            i++;
          }
        }
      }
    }

    if (items > 0 && result != null && !result.isEmpty()) return result;
    return null;
  }

  private List stackToList(Collection stack) {
    if (stack instanceof List) {
      return (List) stack;
    }

    return (List) stack.stream().collect(Collectors.toList());
  }
}
