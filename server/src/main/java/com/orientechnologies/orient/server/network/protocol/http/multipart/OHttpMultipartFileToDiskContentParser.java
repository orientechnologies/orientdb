/*
 *
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Map;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class OHttpMultipartFileToDiskContentParser
    implements OHttpMultipartContentParser<InputStream> {

  protected boolean overwrite = false;
  protected String path;

  public OHttpMultipartFileToDiskContentParser(String iPath) {
    path = iPath;
    if (!path.endsWith("/")) path += "/";
    new File(path).mkdirs();
  }

  @Override
  public InputStream parse(
      final OHttpRequest iRequest,
      final Map<String, String> headers,
      final OHttpMultipartContentInputStream in,
      ODatabaseDocument database)
      throws IOException {
    final StringWriter buffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(buffer);
    json.beginObject();
    String fileName = headers.get(OHttpUtils.MULTIPART_CONTENT_FILENAME);
    int fileSize = 0;

    if (fileName.charAt(0) == '"') fileName = fileName.substring(1);

    if (fileName.charAt(fileName.length() - 1) == '"')
      fileName = fileName.substring(0, fileName.length() - 1);

    fileName = path + fileName;

    if (!overwrite)
      // CHANGE THE FILE NAME TO AVOID OVERWRITING
      if (new File(fileName).exists()) {
        final String fileExt = fileName.substring(fileName.lastIndexOf("."));
        final String fileNoExt = fileName.substring(0, fileName.lastIndexOf("."));

        for (int i = 1; ; ++i) {
          if (!new File(fileNoExt + "_" + i + fileExt).exists()) {
            fileName = fileNoExt + "_" + i + fileExt;
            break;
          }
        }
      }

    // WRITE THE FILE
    final OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName.toString()));
    try {
      int b;
      while ((b = in.read()) > -1) {
        out.write(b);
        fileSize++;
      }
    } finally {
      out.flush();
      out.close();
    }

    // FORMAT THE RETURNING DOCUMENT
    json.writeAttribute(1, true, "name", fileName);
    json.writeAttribute(1, true, "type", headers.get(OHttpUtils.MULTIPART_CONTENT_TYPE));
    json.writeAttribute(1, true, "size", fileSize);
    json.endObject();
    return new ByteArrayInputStream(buffer.toString().getBytes());
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public OHttpMultipartFileToDiskContentParser setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
    return this;
  }
}
