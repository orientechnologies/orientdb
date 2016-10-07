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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.serialization.OStreamable;
import com.orientechnologies.orient.core.serialization.OStreamableHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of distributed transaction.
 * 
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OTxTaskResult implements OStreamable {
  public final List<Object> results = new ArrayList<Object>();

  public OTxTaskResult() {
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof OTxTaskResult))
      return false;

    final OTxTaskResult other = (OTxTaskResult) obj;
    if (results.size() != other.results.size())
      return false;

    for (int i = 0; i < results.size(); ++i) {
      final Object currentResult = results.get(i);
      final Object otherResult = other.results.get(i);
      if (!currentResult.equals(otherResult)) {
        if (OTxTask.NON_LOCAL_CLUSTER.equals(currentResult) || OTxTask.NON_LOCAL_CLUSTER.equals(otherResult))
          continue;
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    return results.hashCode();
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeInt(results.size());
    for (Object o : results)
      OStreamableHelper.toStream(out, o);
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    final int resultSize = in.readInt();
    for (int i = 0; i < resultSize; ++i)
      results.add(OStreamableHelper.fromStream(in));
  }

  /**
   * Prints the transaction content. This is useful in case of conflict to see what is different.
   */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(4192);
    buffer.append("TX[");
    buffer.append(results.size());
    buffer.append("]{");
    int i = 0;
    for (Object o : results) {
      if (i++ > 0)
        buffer.append(',');
      buffer.append(o);
    }
    buffer.append("}");

    return buffer.toString();
  }
}
