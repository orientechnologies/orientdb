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
package com.orientechnologies.orient.core.type;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.common.io.OIOUtils;

public class OBuffer implements Externalizable {
  public byte[] buffer;

  /**
   * Constructor used by serialization.
   */
  public OBuffer() {
  }

  public OBuffer(final byte[] buffer) {
    this.buffer = buffer;
  }

  public void readExternal(final ObjectInput iInput) throws IOException, ClassNotFoundException {
    final int bufferLenght = iInput.readInt();
    if (bufferLenght > 0) {
      buffer = new byte[bufferLenght];
      for (int pos = 0, bytesReaded = 0; pos < bufferLenght; pos += bytesReaded) {
        bytesReaded = iInput.read(buffer, pos, buffer.length - pos);
      }
    } else
      buffer = null;
  }

  public void writeExternal(final ObjectOutput iOutput) throws IOException {
    final int bufferLenght = buffer != null ? buffer.length : 0;
    iOutput.writeInt(bufferLenght);
    if (bufferLenght > 0)
      iOutput.write(buffer);
  }

  @Override
  public String toString() {
    return "size:" + (buffer != null ? buffer.length : "empty");
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public void setBuffer(final byte[] buffer) {
    this.buffer = buffer;
  }

  @Override
  public boolean equals(final Object o) {
    if (o==null || !(o instanceof OBuffer))
      return false;

    return OIOUtils.equals(buffer, ((OBuffer) o).buffer);
  }

  @Override
  public int hashCode() {
	// Use of reference hashCode. Usage of deep hashCode should be considered
	return buffer!=null?buffer.hashCode():0;
  }
  
  
}
