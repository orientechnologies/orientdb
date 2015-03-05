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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects the changes to an index for a certain key
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OTransactionIndexChangesPerKey {
  public Object                       key;
  public List<OTransactionIndexEntry> entries;

  public static class OTransactionIndexEntry {
    public OPERATION     operation;
    public OIdentifiable value;

    public OTransactionIndexEntry(final OIdentifiable iValue, final OPERATION iOperation) {
      value = iValue;
      operation = iOperation;
    }
  }

  public OTransactionIndexChangesPerKey(final Object iKey) {
    this.key = iKey;
    entries = new ArrayList<OTransactionIndexEntry>();
  }

  public void add(OIdentifiable iValue, final OPERATION iOperation) {
    entries.add(new OTransactionIndexEntry(iValue != null ? iValue.getIdentity() : null, iOperation));
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder(64);
    builder.append(key).append(" [");
    boolean first = true;
    for (OTransactionIndexEntry entry : entries) {
      if (first) {
          first = false;
      } else {
          builder.append(',');
      }

      builder.append(entry.value).append(" (").append(entry.operation).append(")");
    }
    builder.append("]");
    return builder.toString();
  }
}
