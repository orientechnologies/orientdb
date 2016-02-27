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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.orientechnologies.orient.core.id.ORID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Result of distributed transaction.
 * 
 * @author Luca Garulli
 */
public class OTxTaskResult implements DataSerializable {
  public final List<Object> results = new ArrayList<Object>();
  public final Set<ORID>    locks   = new HashSet<ORID>();

  public OTxTaskResult() {
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof OTxTaskResult))
      return false;

    return results.equals(((OTxTaskResult) obj).results);
  }

  @Override
  public int hashCode() {
    return results.hashCode();
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
    out.writeInt(results.size());
    for (Object o : results)
      out.writeObject(o);
    out.writeInt(locks.size());
    for (ORID r : locks)
      out.writeObject(r);
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
    final int resultSize = in.readInt();
    for (int i = 0; i < resultSize; ++i)
      results.add(in.readObject());
    final int locksSize = in.readInt();
    for (int i = 0; i < locksSize; ++i)
      locks.add((ORID) in.readObject());
  }

  @Override
  public String toString() {
    return "TX[result=" + results.size() + ", locks=" + locks.size() + "]";
  }
}
