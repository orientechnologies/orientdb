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
package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAll;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAny;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class OTraverseRecordProcess extends OTraverseAbstractProcess<OIdentifiable> {
  private final OTraversePath path;

  public OTraverseRecordProcess(final OTraverse iCommand, final OIdentifiable iTarget, OTraversePath parentPath) {
    super(iCommand, iTarget);
    this.path = parentPath.append(iTarget);
  }

  public OIdentifiable process() {
    if (target == null)
      return pop();

    final int depth = path.getDepth();

    if (command.getContext().isAlreadyTraversed(target, depth))
      // ALREADY EVALUATED, DON'T GO IN DEEP
      return drop();

    if (command.getPredicate() != null) {
      final Object conditionResult = command.getPredicate().evaluate(target, null, command.getContext());
      if (conditionResult != Boolean.TRUE)
        return drop();
    }

    // UPDATE ALL TRAVERSED RECORD TO AVOID RECURSION
    command.getContext().addTraversed(target, depth);

    final int maxDepth = command.getMaxDepth();
    if (maxDepth > -1 && depth == maxDepth) {
      // SKIP IT
      pop();
    } else {
      final ORecord targetRec = target.getRecord();
      if (!(targetRec instanceof ODocument))
        // SKIP IT
        return pop();

      final ODocument targetDoc = (ODocument) targetRec;

      // MATCH!
      final List<Object> fields = new ArrayList<Object>();

      // TRAVERSE THE DOCUMENT ITSELF
      for (Object cfgFieldObject : command.getFields()) {
        String cfgField = cfgFieldObject.toString();

        if ("*".equals(cfgField) || OSQLFilterItemFieldAll.FULL_NAME.equalsIgnoreCase(cfgField)
            || OSQLFilterItemFieldAny.FULL_NAME.equalsIgnoreCase(cfgField)) {

          // ADD ALL THE DOCUMENT FIELD
          Collections.addAll(fields, targetDoc.fieldNames());
          break;

        } else {
          // SINGLE FIELD
          final int pos = OStringSerializerHelper
              .parse(cfgField, new StringBuilder(), 0, -1, new char[] { '.' }, true, true, true, 0, true) - 1;
          if (pos > -1) {
            // FOUND <CLASS>.<FIELD>
            final OClass cls = ODocumentInternal.getImmutableSchemaClass(targetDoc);
            if (cls == null)
              // JUMP IT BECAUSE NO SCHEMA
              continue;

            final String className = cfgField.substring(0, pos);
            if (!cls.isSubClassOf(className))
              // JUMP IT BECAUSE IT'S NOT A INSTANCEOF THE CLASS
              continue;

            cfgField = cfgField.substring(pos + 1);

            fields.add(cfgField);
          } else
            fields.add(cfgFieldObject);
        }
      }

      if (command.getStrategy() == OTraverse.STRATEGY.DEPTH_FIRST)
        // REVERSE NAMES TO BE PROCESSED IN THE RIGHT ORDER
        Collections.reverse(fields);

      processFields(fields.iterator());

      if (targetDoc.isEmbedded())
        return null;
    }

    return target;
  }

  private void processFields(Iterator<Object> target) {
    final ODocument doc = this.target.getRecord();

    while (target.hasNext()) {
      Object field = target.next();

      final Object fieldValue;
      if (field instanceof OSQLFilterItem)
        fieldValue = ((OSQLFilterItem) field).getValue(doc, null, null);
      else
        fieldValue = doc.rawField(field.toString());

      if (fieldValue != null) {
        final OTraverseAbstractProcess<?> subProcess;

        if (fieldValue instanceof Iterator<?> || OMultiValue.isMultiValue(fieldValue)) {
          final Iterator<?> coll;
          if (fieldValue instanceof ORecordLazyMultiValue)
            coll = ((ORecordLazyMultiValue) fieldValue).rawIterator();
          else
            coll = OMultiValue.getMultiValueIterator(fieldValue);

          subProcess = new OTraverseMultiValueProcess(command, (Iterator<Object>) coll, getPath().appendField(field.toString()));
        } else if (fieldValue instanceof OIdentifiable && ((OIdentifiable) fieldValue).getRecord() instanceof ODocument) {
          subProcess = new OTraverseRecordProcess(command, (ODocument) ((OIdentifiable) fieldValue).getRecord(), getPath()
              .appendField(field.toString()));
        } else
          continue;

        command.getContext().push(subProcess);
      }
    }
  }

  @Override
  public String toString() {
    return target != null ? target.getIdentity().toString() : "-";
  }

  @Override
  public OTraversePath getPath() {
    return path;
  }

  public OIdentifiable drop() {
    command.getContext().pop(null);
    return null;
  }

  @Override
  public OIdentifiable pop() {
    command.getContext().pop(target);
    return null;
  }
}
