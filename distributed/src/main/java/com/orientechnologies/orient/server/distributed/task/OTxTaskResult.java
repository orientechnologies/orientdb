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
package com.orientechnologies.orient.server.distributed.task;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of distributed transaction.
 * 
 * @author Luca Garulli
 */
public class OTxTaskResult implements Externalizable {
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
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeInt(results.size());
    for (Object o : results)
      out.writeObject(o);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    final int resultSize = in.readInt();
    for (int i = 0; i < resultSize; ++i)
      results.add(in.readObject());
  }

  @Override
  public String toString() {
    return "TX[result=" + results.size() + "]";
  }
}
