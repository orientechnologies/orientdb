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
package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Iterator;

public class OTraverseRecordSetProcess extends OTraverseAbstractProcess<Iterator<OIdentifiable>> {
  private final OTraversePath path;
  protected OIdentifiable record;
  protected int index = -1;

  public OTraverseRecordSetProcess(
      final OTraverse iCommand, final Iterator<OIdentifiable> iTarget, OTraversePath parentPath) {
    super(iCommand, iTarget);
    this.path = parentPath.appendRecordSet();
    command.getContext().push(this);
  }

  @SuppressWarnings("unchecked")
  public OIdentifiable process() {
    while (target.hasNext()) {
      record = target.next();
      index++;

      final ORecord rec = record.getRecord();
      if (rec instanceof ODocument) {
        ODocument doc = (ODocument) rec;
        if (!doc.getIdentity().isPersistent() && doc.fields() == 1) {
          // EXTRACT THE FIELD CONTEXT
          Object fieldvalue = doc.field(doc.fieldNames()[0]);
          if (fieldvalue instanceof Collection<?>) {
            command
                .getContext()
                .push(
                    new OTraverseRecordSetProcess(
                        command, ((Collection<OIdentifiable>) fieldvalue).iterator(), getPath()));

          } else if (fieldvalue instanceof ODocument) {
            command
                .getContext()
                .push(new OTraverseRecordProcess(command, (ODocument) rec, getPath()));
          }
        } else {
          command
              .getContext()
              .push(new OTraverseRecordProcess(command, (ODocument) rec, getPath()));
        }

        return null;
      }
    }

    return pop();
  }

  @Override
  public OTraversePath getPath() {
    return path;
  }

  @Override
  public String toString() {
    return target != null ? target.toString() : "-";
  }
}
