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
package com.orientechnologies.orient.core.id;

import com.orientechnologies.orient.core.serialization.OMemoryStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Immutable ORID implementation. To be really immutable fields must not be public anymore. TODO!
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OImmutableRecordId extends ORecordId {
  private static final long serialVersionUID = 1L;

  public OImmutableRecordId(final int iClusterId, final long iClusterPosition) {
    super(iClusterId, iClusterPosition);
  }

  public OImmutableRecordId(final ORecordId iRID) {
    super(iRID);
  }

  @Override
  public void copyFrom(final ORID iSource) {
    throw new UnsupportedOperationException("copyFrom");
  }

  @Override
  public ORecordId fromStream(byte[] iBuffer) {
    throw new UnsupportedOperationException("fromStream");
  }

  @Override
  public ORecordId fromStream(OMemoryStream iStream) {
    throw new UnsupportedOperationException("fromStream");
  }

  @Override
  public ORecordId fromStream(InputStream iStream) throws IOException {
    throw new UnsupportedOperationException("fromStream");
  }

  @Override
  public void fromString(String iRecordId) {
    throw new UnsupportedOperationException("fromString");
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("reset");
  }
}
