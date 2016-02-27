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

package com.orientechnologies.orient.server.distributed;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Immutable Serializable wrapper.
 * 
 * @author Luca Garulli
 */
public class OSerializableWrappedResponse implements DataSerializable {
  private Serializable wrapped;

  /**
   * Constructor for unmarshalling.
   */
  public OSerializableWrappedResponse() {
  }

  public OSerializableWrappedResponse(final Serializable e) {
    wrapped = e;
  }

  public Serializable getWrapped() {
    return wrapped;
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
    out.writeObject(wrapped);
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
    wrapped = in.readObject();
  }
}
