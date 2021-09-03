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
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

/** @author Luca Molino (molino.luca--at--gmail.com) */
public abstract class OHttpMultipartRequestCommand<B, F>
    extends OServerCommandAuthenticatedDbAbstract {

  private STATUS parseStatus = STATUS.STATUS_EXPECTED_BOUNDARY;

  protected static enum STATUS {
    STATUS_EXPECTED_BOUNDARY,
    STATUS_EXPECTED_BOUNDARY_CRLF,
    STATUS_EXPECTED_PART_HEADERS,
    STATUS_EXPECTED_PART_CONTENT,
    STATUS_EXPECTED_END_REQUEST
  }

  public HashMap<String, String> parse(
      final OHttpRequest iRequest,
      final OHttpResponse iResponse,
      final OHttpMultipartContentParser<B> standardContentParser,
      final OHttpMultipartContentParser<F> fileContentParser,
      final ODatabaseDocument database)
      throws Exception {
    char currChar;
    boolean endRequest = false;
    final OHttpMultipartContentInputStream contentIn =
        new OHttpMultipartContentInputStream(iRequest.getMultipartStream(), iRequest.getBoundary());
    final HashMap<String, String> headers = new LinkedHashMap<String, String>();
    int in;
    try {
      while (!endRequest && (in = iRequest.getMultipartStream().read()) > 0) {
        currChar = (char) in;
        switch (parseStatus) {
          case STATUS_EXPECTED_BOUNDARY:
            {
              readBoundary(iRequest, iResponse, currChar);
              parseStatus = STATUS.STATUS_EXPECTED_BOUNDARY_CRLF;
              break;
            }

          case STATUS_EXPECTED_BOUNDARY_CRLF:
            {
              endRequest = readBoundaryCrLf(iRequest, iResponse, currChar, endRequest);
              parseStatus = STATUS.STATUS_EXPECTED_PART_HEADERS;
              break;
            }

          case STATUS_EXPECTED_PART_HEADERS:
            {
              parsePartHeaders(iRequest, iResponse, currChar, endRequest, headers);
              parseStatus = STATUS.STATUS_EXPECTED_PART_CONTENT;
              break;
            }

          case STATUS_EXPECTED_PART_CONTENT:
            {
              iRequest.getMultipartStream().setSkipInput(in);
              contentIn.reset();
              if (headers.get(OHttpUtils.MULTIPART_CONTENT_FILENAME) != null) {
                parseFileContent(iRequest, fileContentParser, headers, contentIn, database);
              } else {
                parseBaseContent(iRequest, standardContentParser, headers, contentIn, database);
              }
              break;
            }

          case STATUS_EXPECTED_END_REQUEST:
            {
              iRequest.getMultipartStream().setSkipInput(in);
              endRequest = OHttpMultipartHelper.isEndRequest(iRequest);
              if (!endRequest) {
                parseStatus = STATUS.STATUS_EXPECTED_BOUNDARY_CRLF;
              } else {
                parseStatus = STATUS.STATUS_EXPECTED_BOUNDARY;
              }
              break;
            }
        }
      }
      parseStatus = STATUS.STATUS_EXPECTED_BOUNDARY;
    } catch (Exception e) {
      throw e;
    }

    return headers;
  }

  protected boolean readBoundaryCrLf(
      final OHttpRequest iRequest, final OHttpResponse iResponse, char currChar, boolean endRequest)
      throws IOException {
    int in;
    if (currChar == '\r') {
      in = iRequest.getMultipartStream().read();
      currChar = (char) in;
      if (currChar == '\n') {
        return false;
      }
    } else if (currChar == '-') {
      in = iRequest.getMultipartStream().read();
      currChar = (char) in;
      if (currChar == '-') {
        endRequest = true;
      } else {
        iResponse.send(
            OHttpUtils.STATUS_INVALIDMETHOD_CODE,
            "Wrong request: Expected -",
            OHttpUtils.CONTENT_TEXT_PLAIN,
            "Wrong request: Expected -",
            null);
        endRequest = true;
      }
    } else {
      iResponse.send(
          OHttpUtils.STATUS_INVALIDMETHOD_CODE,
          "Wrong request: Expected CR/LF",
          OHttpUtils.CONTENT_TEXT_PLAIN,
          "Wrong request: Expected CR/LF",
          null);
      endRequest = true;
    }
    return endRequest;
  }

  protected void readBoundary(
      final OHttpRequest iRequest, final OHttpResponse iResponse, char currChar)
      throws IOException {
    int in;
    int boundaryCursor = 0;
    for (int i = 0; i < 2; i++) {
      if (currChar != '-') {
        iResponse.send(
            OHttpUtils.STATUS_INVALIDMETHOD_CODE,
            "Wrong request: Expected boundary",
            OHttpUtils.CONTENT_TEXT_PLAIN,
            "Wrong request: Expected boundary",
            null);
        return;
      }
      in = iRequest.getMultipartStream().read();
      currChar = (char) in;
    }
    while (boundaryCursor < iRequest.getBoundary().length()) {
      if (currChar != iRequest.getBoundary().charAt(boundaryCursor)) {
        iResponse.send(
            OHttpUtils.STATUS_INVALIDMETHOD_CODE,
            "Wrong request: Expected boundary",
            OHttpUtils.CONTENT_TEXT_PLAIN,
            "Wrong request: Expected boundary",
            null);
      }
      boundaryCursor++;
      if (boundaryCursor < iRequest.getBoundary().length()) {
        in = iRequest.getMultipartStream().read();
        currChar = (char) in;
      }
    }
  }

  protected void parsePartHeaders(
      final OHttpRequest iRequest,
      final OHttpResponse iResponse,
      char currChar,
      boolean endRequest,
      final HashMap<String, String> headers)
      throws IOException {
    int in;
    StringBuilder headerName = new StringBuilder();
    boolean endOfHeaders = false;
    while (!endOfHeaders) {
      headerName.append(currChar);
      if (OHttpMultipartHelper.isMultipartPartHeader(headerName)) {
        currChar = parseHeader(iRequest, iResponse, headers, headerName.toString());
        headerName.setLength(0);
      }
      if (currChar == '\r') {
        in = iRequest.getMultipartStream().read();
        currChar = (char) in;
        if (currChar == '\n') {
          in = iRequest.getMultipartStream().read();
          currChar = (char) in;
          if (currChar == '\r') {
            in = iRequest.getMultipartStream().read();
            currChar = (char) in;
            if (currChar == '\n') {
              endOfHeaders = true;
            }
          }
        }
      } else {
        in = iRequest.getMultipartStream().read();
        currChar = (char) in;
      }
    }
  }

  protected char parseHeader(
      final OHttpRequest iRequest,
      final OHttpResponse iResponse,
      HashMap<String, String> headers,
      final String headerName)
      throws IOException {
    final StringBuilder header = new StringBuilder();
    boolean endOfHeader = false;
    int in;
    char currChar;
    in = iRequest.getMultipartStream().read();
    currChar = (char) in;
    if (currChar == ':') {
      in = iRequest.getMultipartStream().read();
      currChar = (char) in;
      if (currChar != ' ') {
        iResponse.send(
            OHttpUtils.STATUS_INVALIDMETHOD_CODE,
            "Wrong request part header: Expected ' ' (header: " + headerName + ")",
            OHttpUtils.CONTENT_TEXT_PLAIN,
            "Wrong request part header: Expected ' ' (header: " + headerName + ")",
            null);
      }
    } else if (currChar != '=') {
      iResponse.send(
          OHttpUtils.STATUS_INVALIDMETHOD_CODE,
          "Wrong request part header: Expected ':' (header: " + headerName + ")",
          OHttpUtils.CONTENT_TEXT_PLAIN,
          "Wrong request part header: Expected ':' (header: " + headerName + ")",
          null);
    }
    while (!endOfHeader) {
      in = iRequest.getMultipartStream().read();
      currChar = (char) in;
      if (currChar == ';') {
        if (header.charAt(0) == '"') {
          header.deleteCharAt(0);
        }
        if (header.charAt(header.length() - 1) == '"') {
          header.deleteCharAt(header.length() - 1);
        }
        headers.put(headerName, header.toString());
        in = iRequest.getMultipartStream().read();
        return (char) in;
      } else if (currChar == '\r') {
        if (header.charAt(0) == '"') {
          header.deleteCharAt(0);
        }
        if (header.charAt(header.length() - 1) == '"') {
          header.deleteCharAt(header.length() - 1);
        }
        headers.put(headerName, header.toString());
        return currChar;
      }
      header.append(currChar);
    }
    return currChar;
  }

  protected void parseBaseContent(
      final OHttpRequest iRequest,
      final OHttpMultipartContentParser<B> contentParser,
      final HashMap<String, String> headers,
      final OHttpMultipartContentInputStream in,
      ODatabaseDocument database)
      throws Exception {
    B result = contentParser.parse(iRequest, headers, in, database);
    parseStatus = STATUS.STATUS_EXPECTED_END_REQUEST;
    processBaseContent(iRequest, result, headers);
  }

  protected void parseFileContent(
      final OHttpRequest iRequest,
      final OHttpMultipartContentParser<F> contentParser,
      final HashMap<String, String> headers,
      final OHttpMultipartContentInputStream in,
      ODatabaseDocument database)
      throws Exception {
    F result = contentParser.parse(iRequest, headers, in, database);
    parseStatus = STATUS.STATUS_EXPECTED_END_REQUEST;
    processFileContent(iRequest, result, headers);
  }

  protected abstract void processBaseContent(
      final OHttpRequest iRequest, B iContentResult, HashMap<String, String> headers)
      throws Exception;

  protected abstract void processFileContent(
      final OHttpRequest iRequest, F iContentResult, HashMap<String, String> headers)
      throws Exception;

  protected abstract String getFileParamenterName();

  protected abstract String getDocumentParamenterName();
}
