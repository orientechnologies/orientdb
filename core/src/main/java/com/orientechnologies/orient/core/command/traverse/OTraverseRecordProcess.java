/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.command.traverse;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAll;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemFieldAny;

public class OTraverseRecordProcess extends OTraverseAbstractProcess<ODocument> {
  private boolean skipDocument = false;

  /**
   * @param iCommand
   * @param iTarget
   */
  public OTraverseRecordProcess(final OTraverse iCommand, final ODocument iTarget) {
    this(iCommand, iTarget, false);
  }

  public OTraverseRecordProcess(final OTraverse iCommand, final ODocument iTarget, final boolean iSkipDocument) {
    super(iCommand, iTarget);
    skipDocument = iSkipDocument;
    if (!skipDocument)
      command.getContext().incrementDepth();
  }

  public OIdentifiable process() {
    if (target == null)
      return drop();

    if (command.getContext().isAlreadyTraversed(target))
      // ALREADY EVALUATED, DON'T GO IN DEEP
      return drop();

    if (target.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
      try {
        target.reload();
      } catch (final ORecordNotFoundException e) {
        // INVALID RID
        return drop();
      }

    if (command.getPredicate() != null) {
      final Object conditionResult = command.getPredicate().evaluate(target, null, command.getContext());
      if (conditionResult != Boolean.TRUE)
        return drop();
    }

    // UPDATE ALL TRAVERSED RECORD TO AVOID RECURSION
    command.getContext().addTraversed(target);

    // MATCH!
    final List<Object> fields = new ArrayList<Object>();

    // TRAVERSE THE DOCUMENT ITSELF
    for (Object cfgFieldObject : command.getFields()) {
      String cfgField = cfgFieldObject.toString();

      if ("*".equals(cfgField) || OSQLFilterItemFieldAll.FULL_NAME.equalsIgnoreCase(cfgField)
          || OSQLFilterItemFieldAny.FULL_NAME.equalsIgnoreCase(cfgField)) {

        // ADD ALL THE DOCUMENT FIELD
        for (String f : target.fieldNames())
          fields.add(f);

        break;

      } else {
        // SINGLE FIELD
        final int pos = cfgField.indexOf('.');
        if (pos > -1) {
          // FOUND <CLASS>.<FIELD>
          final OClass cls = target.getSchemaClass();
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

    final OTraverseFieldProcess field = new OTraverseFieldProcess(command, fields.iterator());

    if (skipDocument) {
      // GO DIRECTLY TO THE FIELD
      final OIdentifiable res = field.process();
      if (res != null)
        return res;
      return drop();
    } else
      // RETURN THE DOCUMENT ITSELF
      return target;
  }

  @Override
  public String getStatus() {
    return target != null ? target.getIdentity().toString() : null;
  }

  @Override
  public String toString() {
    return target != null ? target.getIdentity().toString() : "-";
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.command.traverse.OTraverseAbstractProcess#drop()
   */
  @Override
  public OIdentifiable drop() {
    if (!skipDocument)
      command.getContext().decrementDepth();
    return super.drop();
  }
}