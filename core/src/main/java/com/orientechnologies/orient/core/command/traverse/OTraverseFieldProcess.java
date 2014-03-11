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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;

public class OTraverseFieldProcess extends OTraverseAbstractProcess<Iterator<Object>> {
  private final List<String> parentPath;
  private final ODocument    doc;
  protected Object           field;

  public OTraverseFieldProcess(final OTraverse iCommand, ODocument doc, final Iterator<Object> iTarget, List<String> parentPath) {
    super(iCommand, iTarget);
    this.parentPath = parentPath;
    this.doc = doc;
  }

  public OIdentifiable process() {
    while (target.hasNext()) {
      field = target.next();

      final Object fieldValue;
      if (field instanceof OSQLFilterItem)
        fieldValue = ((OSQLFilterItem) field).getValue(doc, null, null);
      else
        fieldValue = doc.rawField(field.toString());

      if (fieldValue != null) {
        final OTraverseAbstractProcess<?> subProcess;

        if (fieldValue instanceof Iterator<?> || OMultiValue.isMultiValue(fieldValue)) {
          final Iterator<Object> coll = OMultiValue.getMultiValueIterator(fieldValue);

          subProcess = new OTraverseMultiValueProcess(command, coll, getPath());
        } else if (fieldValue instanceof OIdentifiable && ((OIdentifiable) fieldValue).getRecord() instanceof ODocument) {
          subProcess = new OTraverseRecordProcess(command, (ODocument) ((OIdentifiable) fieldValue).getRecord(), parentPath);
        }        else
          continue;

        command.getContext().push(subProcess);

        return null;
     }
    }

    return drop();
  }

  @Override
  public String getStatus() {
    return field != null ? field.toString() : null;
  }

  @Override
  public List<String> getPath() {
    if (field != null) {
      final ArrayList<String> path = new ArrayList<String>(parentPath);
      path.add(field.toString());
      return path;
    } else
      return null;
  }

  @Override
  public String toString() {
    return field != null ? "[field:" + field.toString() + "]" : null;
  }
}
