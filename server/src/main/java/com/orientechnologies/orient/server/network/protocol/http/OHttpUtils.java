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
package com.orientechnologies.orient.server.network.protocol.http;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains HTTP utilities static methods and constants.
 * 
 * @author Luca Garulli
 * 
 */
public class OHttpUtils {

  public static final String URL_SEPARATOR                       = "/";
  public static final char   URL_SEPARATOR_CHAR                  = '/';
  public static final byte[] EOL                                 = { (byte) '\r', (byte) '\n' };

  public static final String METHOD_GET                          = "GET";
  public static final String METHOD_PUT                          = "PUT";
  public static final String METHOD_DELETE                       = "DELETE";
  public static final String METHOD_POST                         = "POST";

  public static final String HEADER_CONTENT_LENGTH               = "Content-Length: ";
  public static final String HEADER_CONTENT_TYPE                 = "Content-Type: ";
  public static final String HEADER_COOKIE                       = "Cookie: ";
  public static final String HEADER_AUTHORIZATION                = "Authorization: ";
  public static final String HEADER_IF_MATCH                     = "If-Match: ";
  public static final String HEADER_X_FORWARDED_FOR              = "X-Forwarded-For: ";
  public static final String HEADER_AUTHENTICATION               = "OAuthentication: ";
  public static final String HEADER_CONTENT_ENCODING             = "Accept-Encoding: ";

  public static final String AUTHORIZATION_BASIC                 = "Basic";
  public static final String OSESSIONID                          = "OSESSIONID";

  public static final String MULTIPART_CONTENT_DISPOSITION       = "Content-Disposition";
  public static final String MULTIPART_CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";
  public static final String MULTIPART_CONTENT_CHARSET           = "charset";
  public static final String MULTIPART_CONTENT_FILENAME          = "filename";
  public static final String MULTIPART_CONTENT_NAME              = "name";
  public static final String MULTIPART_CONTENT_TYPE              = "Content-Type";

  public static final String CONTENT_TYPE_MULTIPART              = "multipart/form-data";
  public static final String CONTENT_TYPE_URLENCODED             = "application/x-www-form-urlencoded";
  public static final String BOUNDARY                            = "boundary";

  public static final String CONTENT_TEXT_PLAIN                  = "text/plain";
  public static final String CONTENT_JSON                        = "application/json";
  public static final String CONTENT_JAVASCRIPT                  = "text/javascript";
  public static final String CONTENT_GZIP                        = "application/x-gzip";
  public static final String CONTENT_ACCEPT_GZIP_ENCODED         = "gzip";

  public static final String CALLBACK_PARAMETER_NAME             = "callback";

  public static final int    STATUS_CREATED_CODE                 = 201;
  public static final String STATUS_CREATED_DESCRIPTION          = "Created";
  public static final int    STATUS_OK_CODE                      = 200;
  public static final String STATUS_OK_DESCRIPTION               = "OK";
  public static final int    STATUS_OK_NOCONTENT_CODE            = 204;
  public static final String STATUS_OK_NOCONTENT_DESCRIPTION     = "OK";
  public static final int    STATUS_BADREQ_CODE                  = 400;
  public static final String STATUS_BADREQ_DESCRIPTION           = "Bad request";
  public static final int    STATUS_AUTH_CODE                    = 401;
  public static final String STATUS_AUTH_DESCRIPTION             = "Unauthorized";
  public static final int    STATUS_FORBIDDEN_CODE               = 403;
  public static final String STATUS_FORBIDDEN_DESCRIPTION        = "Forbidden";
  public static final int    STATUS_NOTFOUND_CODE                = 404;
  public static final String STATUS_NOTFOUND_DESCRIPTION         = "Not Found";
  public static final int    STATUS_INVALIDMETHOD_CODE           = 405;
  public static final String STATUS_INVALIDMETHOD_DESCRIPTION    = "Method Not Allowed";
  public static final int    STATUS_CONFLICT_CODE                = 409;
  public static final String STATUS_CONFLICT_DESCRIPTION         = "Conflict";
  public static final int    STATUS_INTERNALERROR_CODE           = 500;
  public static final String STATUS_INTERNALERROR_DESCRIPTION    = "Internal Server Error";
  public static final int    STATUS_NOTIMPL_CODE                 = 501;
  public static final String STATUS_NOTIMPL_DESCRIPTION          = "Not Implemented";

  protected static Map<String, String> getParameters(final String iURL) {
    int begin = iURL.indexOf("?");
    if (begin > -1) {
      Map<String, String> params = new HashMap<String, String>();
      String parameters = iURL.substring(begin + 1);
      final String[] paramPairs = parameters.split("&");
      for (String p : paramPairs) {
        final String[] parts = p.split("=");
        if (parts.length == 2)
          params.put(parts[0], parts[1]);
      }
      return params;
    }
    return Collections.emptyMap();
  }

  public static String nextChainUrl(final String iCurrentUrl) {
    if (!iCurrentUrl.contains("/"))
      return iCurrentUrl;

    return iCurrentUrl.startsWith("/") ? iCurrentUrl.substring(iCurrentUrl.indexOf('/', 1)) : iCurrentUrl.substring(iCurrentUrl
        .indexOf("/"));
  }
}
