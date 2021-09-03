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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartContentBaseParser;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartDatabaseImportContentParser;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartRequestCommand;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public class OServerCommandPostImportDatabase
    extends OHttpMultipartRequestCommand<String, InputStream> implements OCommandOutputListener {

  protected static final String[] NAMES = {"POST|import/*"};
  protected StringWriter buffer;
  protected InputStream importData;
  protected ODatabaseDocumentInternal database;

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    if (!iRequest.isMultipart()) {
      database = getProfiledDatabaseInstance(iRequest);
      try {
        ODatabaseImport importer =
            new ODatabaseImport(
                database, new ByteArrayInputStream(iRequest.getContent().getBytes("UTF8")), this);
        for (Map.Entry<String, String> option : iRequest.getParameters().entrySet())
          importer.setOption(option.getKey(), option.getValue());
        importer.importDatabase();

        iResponse.send(
            OHttpUtils.STATUS_OK_CODE,
            OHttpUtils.STATUS_OK_DESCRIPTION,
            OHttpUtils.CONTENT_JSON,
            "{\"responseText\": \"Database imported Correctly, see server log for more informations.\"}",
            null);
      } catch (Exception e) {
        iResponse.send(
            OHttpUtils.STATUS_INTERNALERROR_CODE,
            e.getMessage() + ": " + e.getCause() != null ? e.getCause().getMessage() : "",
            OHttpUtils.CONTENT_JSON,
            "{\"responseText\": \""
                + e.getMessage()
                + ": "
                + (e.getCause() != null ? e.getCause().getMessage() : "")
                + "\"}",
            null);
      } finally {
        if (database != null) database.close();
        database = null;
      }
    } else if (iRequest.getMultipartStream() == null
        || iRequest.getMultipartStream().available() <= 0) {
      iResponse.send(
          OHttpUtils.STATUS_INVALIDMETHOD_CODE,
          "Content stream is null or empty",
          OHttpUtils.CONTENT_TEXT_PLAIN,
          "Content stream is null or empty",
          null);
    } else {
      database = getProfiledDatabaseInstance(iRequest);
      try {
        parse(
            iRequest,
            iResponse,
            new OHttpMultipartContentBaseParser(),
            new OHttpMultipartDatabaseImportContentParser(),
            database);

        ODatabaseImport importer = new ODatabaseImport(database, importData, this);
        for (Map.Entry<String, String> option : iRequest.getParameters().entrySet())
          importer.setOption(option.getKey(), option.getValue());
        importer.importDatabase();

        iResponse.send(
            OHttpUtils.STATUS_OK_CODE,
            OHttpUtils.STATUS_OK_DESCRIPTION,
            OHttpUtils.CONTENT_JSON,
            "{\"responseText\": \"Database imported Correctly, see server log for more informations.\"}",
            null);
      } catch (Exception e) {
        iResponse.send(
            OHttpUtils.STATUS_INTERNALERROR_CODE,
            e.getMessage() + ": " + e.getCause() != null ? e.getCause().getMessage() : "",
            OHttpUtils.CONTENT_JSON,
            "{\"responseText\": \""
                + e.getMessage()
                + ": "
                + (e.getCause() != null ? e.getCause().getMessage() : "")
                + "\"}",
            null);
      } finally {
        if (database != null) database.close();
        database = null;
        if (importData != null) importData.close();
        importData = null;
      }
    }
    return false;
  }

  @Override
  protected void processBaseContent(
      final OHttpRequest iRequest,
      final String iContentResult,
      final HashMap<String, String> headers)
      throws Exception {}

  @Override
  protected void processFileContent(
      final OHttpRequest iRequest,
      final InputStream iContentResult,
      final HashMap<String, String> headers)
      throws Exception {
    importData = iContentResult;
  }

  @Override
  protected String getDocumentParamenterName() {
    return "linkValue";
  }

  @Override
  protected String getFileParamenterName() {
    return "databaseFile";
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  @Override
  public void onMessage(String iText) {
    final String msg = iText.startsWith("\n") ? iText.substring(1) : iText;
    OLogManager.instance().info(this, msg, OCommonConst.EMPTY_OBJECT_ARRAY);
  }
}
