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
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OTraverseMultiValueBreadthFirstProcess extends OTraverseAbstractProcess<Iterator<Object>> {
  protected Object       value;
  protected int          index   = -1;
  protected List<Object> sub     = new ArrayList<Object>();
  protected boolean      shallow = true;

  public OTraverseMultiValueBreadthFirstProcess(final OTraverse iCommand, final Iterator<Object> iTarget) {
    super(iCommand, iTarget);
    command.getContext().incrementDepth();
  }

  public OIdentifiable process() {
    if (shallow) {
      // RETURNS THE SHALLOW LEVEL FIRST
      while (target.hasNext()) {
        value = target.next();
        index++;

        if (value instanceof OIdentifiable) {
          final ORecord<?> rec = ((OIdentifiable) value).getRecord();
          if (rec instanceof ODocument) {
            if (command.getPredicate() != null) {
              final Object conditionResult = command.getPredicate().evaluate(rec, null, command.getContext());
              if (conditionResult != Boolean.TRUE)
                continue;
            }

            sub.add(rec);
            return rec;
          }
        }
      }

      target = sub.iterator();
      index = -1;
      shallow = false;
    }

    // SHALLOW DONE, GO IN DEEP
    while (target.hasNext()) {
      value = target.next();
      index++;

      final OTraverseRecordProcess subProcess = new OTraverseRecordProcess(command, (ODocument) value, true);

      final OIdentifiable subValue = subProcess.process();
      if (subValue != null)
        return subValue;
    }

    sub = null;
    return drop();
  }

  @Override
  public OIdentifiable drop() {
    command.getContext().decrementDepth();
    return super.drop();
  }

  @Override
  public String getStatus() {
    return toString();
  }

  @Override
  public String toString() {
    return "[" + index + "]";
  }
}