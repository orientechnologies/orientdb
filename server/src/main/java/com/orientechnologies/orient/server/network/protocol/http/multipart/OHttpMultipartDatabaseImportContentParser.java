/*
 *
 * Copyright 2011 Luca Molino (molino.luca--AT--gmail.com *
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
package com.orientechnologies.orient.server.network.protocol.http.multipart;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

/**
 * @author luca.molino
 * 
 */
public class OHttpMultipartDatabaseImportContentParser implements OHttpMultipartContentParser<InputStream> {

  @Override
  public InputStream parse(final OHttpRequest iRequest, final Map<String, String> headers,
      final OHttpMultipartContentInputStream in, ODatabaseRecord database) throws IOException {
    final String fileName = headers.get(OHttpUtils.MULTIPART_CONTENT_FILENAME);

    if (fileName.endsWith(".gz") || fileName.endsWith(".gzip"))
      return new GZIPInputStream(in);

    return in;
  }
}
