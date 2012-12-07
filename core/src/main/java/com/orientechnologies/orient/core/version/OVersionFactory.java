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

package com.orientechnologies.orient.core.version;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.util.OHostInfo;

/**
 * 
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 10/25/12
 */
public class OVersionFactory {
  private static final OVersionFactory instance       = new OVersionFactory();
  private static final boolean         useDistributed = OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean();
  private static final long            macAddress     = convertMacToLong(OHostInfo.getMac());

  private static long convertMacToLong(byte[] mac) {
    long result = 0;
    for (int i = mac.length - 1; i >= 0; i--) {
      result = (result << 8) | (mac[i] & 0xFF);
    }
    return result;
  }

  public ORecordVersion createVersion() {
    if (useDistributed) {
      return new ODistributedVersion(0);
    } else {
      return new OSimpleVersion();
    }
  }

  public ORecordVersion createTombstone() {
    if (useDistributed) {
      return new ODistributedVersion(-1);
    } else {
      return new OSimpleVersion(-1);
    }
  }

  public ORecordVersion createUntrackedVersion() {
    if (useDistributed) {
      return new ODistributedVersion(-1);
    } else {
      return new OSimpleVersion(-1);
    }
  }

  public static OVersionFactory instance() {
    return instance;
  }

  long getMacAddress() {
    return macAddress;
  }

  public boolean isDistributed() {
    return useDistributed;
  }

  public int getVersionSize() {
    if (useDistributed)
      return OBinaryProtocol.SIZE_INT + 2 * OBinaryProtocol.SIZE_LONG;
    else
      return OBinaryProtocol.SIZE_INT;
  }

  public ODistributedVersion createDistributedVersion(int recordVersion, long timestamp, long macAddress) {
    return new ODistributedVersion(recordVersion, timestamp, macAddress);
  }
}
