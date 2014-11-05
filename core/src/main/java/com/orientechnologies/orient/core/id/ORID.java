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
package com.orientechnologies.orient.core.id;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

import java.io.IOException;
import java.io.OutputStream;

public interface ORID extends OIdentifiable, OSerializableStream {
  public static final char PREFIX              = '#';
  public static final char SEPARATOR           = ':';
  public static final int  CLUSTER_MAX         = 32767;
  public static final int  CLUSTER_ID_INVALID  = -1;
  public static final long CLUSTER_POS_INVALID = -1;

  public int getClusterId();

  public long getClusterPosition();

  public void reset();

  public boolean isPersistent();

  public boolean isValid();

  public boolean isNew();

  public boolean isTemporary();

  public ORID copy();

  public String next();

  public ORID nextRid();

  public int toStream(OutputStream iStream) throws IOException;

  public StringBuilder toString(StringBuilder iBuffer);
}
