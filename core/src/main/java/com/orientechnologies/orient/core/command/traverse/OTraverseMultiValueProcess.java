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

import com.orientechnologies.orient.core.db.record.OAutoConvertToRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Iterator;

public class OTraverseMultiValueProcess extends OTraverseAbstractProcess<Iterator<Object>> {
  private final OTraversePath parentPath;
  protected Object value;
  protected int index = -1;

  public OTraverseMultiValueProcess(
      final OTraverse iCommand, final Iterator<Object> iTarget, OTraversePath parentPath) {
    super(iCommand, iTarget);
    this.parentPath = parentPath;

    if (target instanceof OAutoConvertToRecord)
      // FORCE AVOIDING TO CONVERT IN RECORD
      ((OAutoConvertToRecord) target).setAutoConvertToRecord(false);
  }

  public OIdentifiable process() {
    while (target.hasNext()) {
      value = target.next();
      index++;

      if (value instanceof OIdentifiable) {

        if (value instanceof ORID) {
          value = ((OIdentifiable) value).getRecord();
        }
        final OTraverseAbstractProcess<OIdentifiable> subProcess =
            new OTraverseRecordProcess(command, (OIdentifiable) value, getPath());
        command.getContext().push(subProcess);

        return null;
      }
    }

    return pop();
  }

  @Override
  public OTraversePath getPath() {
    return parentPath.appendIndex(index);
  }

  @Override
  public String toString() {
    return "[idx:" + index + "]";
  }
}
