/*
 *
 * Copyright 2011 Luca Molino (molino.luca--AT--gmail.com*
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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.IOException;
import java.util.Date;

import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

/**
 * @author luca.molino
 * 
 */
public class OServerCommandGetFileDownload extends OServerCommandAuthenticatedDbAbstract {

  private static final String[] NAMES = { "GET|fileDownload/*" };

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: fileDownload/<database>/rid/[/<fileName>][/<fileType>].");

    final String fileName = urlParts.length > 3 ? encodeResponseText(urlParts[3]) : "unknown";

    final String fileType = urlParts.length > 5 ? encodeResponseText(urlParts[4]) + '/' + encodeResponseText(urlParts[5])
        : (urlParts.length > 4 ? encodeResponseText(urlParts[4]) : "");

    final String rid = urlParts[2];

    iRequest.data.commandInfo = "Download";
    iRequest.data.commandDetail = rid;

    final ORecordAbstract response;
    ODatabaseDocument db = getProfiledDatabaseInstance(iRequest);
    try {

      response = db.load(new ORecordId(rid));
      if (response != null) {
        if (response instanceof OBlob) {
          sendORecordBinaryFileContent(iRequest, iResponse, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, fileType,
              (OBlob) response, fileName);
        } else if (response instanceof ODocument) {
          for (OProperty prop : ODocumentInternal.getImmutableSchemaClass(((ODocument) response)).properties()) {
            if (prop.getType().equals(OType.BINARY))
              sendBinaryFieldFileContent(iRequest, iResponse, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION,
                  fileType, (byte[]) ((ODocument) response).field(prop.getName()), fileName);
          }
        } else {
          iResponse.send(OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Record requested is not a file nor has a readable schema",
              OHttpUtils.CONTENT_TEXT_PLAIN, "Record requested is not a file nor has a readable schema", null);
        }
      } else {
        iResponse.send(OHttpUtils.STATUS_INVALIDMETHOD_CODE, "Record requested not exists", OHttpUtils.CONTENT_TEXT_PLAIN,
            "Record requestes not exists", null);
      }
    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_INTERNALERROR_CODE, OHttpUtils.STATUS_INTERNALERROR_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN, e.getMessage(), null);
    } finally {
      if (db != null)
        db.close();
    }

    return false;
  }

  protected void sendORecordBinaryFileContent(final OHttpRequest iRequest, final OHttpResponse iResponse, final int iCode,
      final String iReason, final String iContentType, final OBlob record, final String iFileName) throws IOException {
    iResponse.writeStatus(iCode, iReason);
    iResponse.writeHeaders(iContentType);
    iResponse.writeLine("Content-Disposition: attachment; filename=" + iFileName);
    iResponse.writeLine("Date: " + new Date());
    iResponse.writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (record.getSize()));
    iResponse.writeLine(null);

    record.toOutputStream(iResponse.getOutputStream());

    iResponse.flush();
  }

  protected void sendBinaryFieldFileContent(final OHttpRequest iRequest, final OHttpResponse iResponse, final int iCode,
      final String iReason, final String iContentType, final byte[] record, final String iFileName) throws IOException {
    iResponse.writeStatus(iCode, iReason);
    iResponse.writeHeaders(iContentType);
    iResponse.writeLine("Content-Disposition: attachment; filename=" + iFileName);
    iResponse.writeLine("Date: " + new Date());
    iResponse.writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (record.length));
    iResponse.writeLine(null);

    iResponse.getOutputStream().write(record);

    iResponse.flush();
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  private String encodeResponseText(String iText) {
    iText = OPatternConst.PATTERN_SINGLE_SPACE.matcher(iText).replaceAll("%20");
    iText = OPatternConst.PATTERN_AMP.matcher(iText).replaceAll("%26");
    return iText;
  }

}
