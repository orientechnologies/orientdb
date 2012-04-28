/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.Iterator;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OTraverseFieldProcess extends OTraverseAbstractProcess<Iterator<String>> {
  protected String fieldName;

  public OTraverseFieldProcess(final OTraverse iCommand, final Iterator<String> iTarget) {
    super(iCommand, iTarget);
  }

  public OIdentifiable process() {
    while (target.hasNext()) {
      fieldName = target.next();

      final Object fieldValue = ((OTraverseRecordProcess) command.getContext().peek(-2)).getTarget().rawField(fieldName);

      if (fieldValue != null) {
        final OTraverseAbstractProcess<?> subProcess;

        if (OMultiValue.isMultiValue(fieldValue))
          subProcess = new OTraverseMultiValueProcess(command, OMultiValue.getMultiValueIterator(fieldValue));
        else if (fieldValue instanceof OIdentifiable)
          subProcess = new OTraverseRecordProcess(command, (ODocument) ((OIdentifiable) fieldValue).getRecord());
        else
          continue;

        final OIdentifiable subValue = subProcess.process();
        if (subValue != null)
          return subValue;
      }
    }

    return drop();
  }

  @Override
  public String getStatus() {
    return fieldName != null ? "[field:" + fieldName + "]" : null;
  }

  @Override
  public String toString() {
    return fieldName != null ? "[field:" + fieldName + "]" : null;
  }
}