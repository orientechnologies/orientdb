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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPOutputStream;

public class OServerCommandGetStaticContent extends OServerCommandConfigurableAbstract {
  private static final String[] DEF_PATTERN = {
    "GET|www",
    "GET|studio/",
    "GET|",
    "GET|*.htm",
    "GET|*.html",
    "GET|*.xml",
    "GET|*.jpeg",
    "GET|*.jpg",
    "GET|*.png",
    "GET|*.gif",
    "GET|*.js",
    "GET|*.otf",
    "GET|*.css",
    "GET|*.swf",
    "GET|favicon.ico",
    "GET|robots.txt"
  };

  private static final String CONFIG_HTTP_CACHE = "http.cache:";
  private static final String CONFIG_ROOT_PATH = "root.path";
  private static final String CONFIG_FILE_PATH = "file.path";

  private ConcurrentHashMap<String, OStaticContentCachedEntry> cacheContents =
      new ConcurrentHashMap<String, OStaticContentCachedEntry>();
  private Map<String, String> cacheHttp = new HashMap<String, String>();
  private String cacheHttpDefault = "Cache-Control: max-age=3000";
  private String rootPath;
  private String filePath;
  private ConcurrentHashMap<String, OCallable<Object, String>> virtualFolders =
      new ConcurrentHashMap<String, OCallable<Object, String>>();

  public static class OStaticContent {
    public InputStream is = null;
    public long contentSize = 0;
    public String type = null;
  }

  public OServerCommandGetStaticContent() {
    super(DEF_PATTERN);
  }

  public OServerCommandGetStaticContent(final OServerCommandConfiguration iConfiguration) {
    super(iConfiguration.pattern);

    // LOAD HTTP CACHE CONFIGURATION
    for (OServerEntryConfiguration par : iConfiguration.parameters) {
      if (par.name.startsWith(CONFIG_HTTP_CACHE)) {
        final String filter = par.name.substring(CONFIG_HTTP_CACHE.length());
        if (filter.equalsIgnoreCase("default")) cacheHttpDefault = par.value;
        else if (filter.length() > 0) {
          final String[] filters = filter.split(" ");
          for (String f : filters) {
            cacheHttp.put(f, par.value);
          }
        }
      } else if (par.name.startsWith(CONFIG_ROOT_PATH)) rootPath = par.value;
      else if (par.name.startsWith(CONFIG_FILE_PATH)) filePath = par.value;
    }
  }

  @Override
  public boolean beforeExecute(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    String header = cacheHttpDefault;

    if (cacheHttp.size() > 0) {
      final String resource = getResource(iRequest);

      // SEARCH IN CACHE IF ANY
      for (Entry<String, String> entry : cacheHttp.entrySet()) {
        final int wildcardPos = entry.getKey().indexOf('*');
        final String partLeft = entry.getKey().substring(0, wildcardPos);
        final String partRight = entry.getKey().substring(wildcardPos + 1);

        if (resource.startsWith(partLeft) && resource.endsWith(partRight)) {
          // FOUND
          header = entry.getValue();
          break;
        }
      }
    }

    iResponse.setHeader(header);
    return true;
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws Exception {
    iRequest.getData().commandInfo = "Get static content";
    iRequest.getData().commandDetail = iRequest.getUrl();

    OStaticContent staticContent = null;
    try {
      staticContent = getVirtualFolderContent(iRequest);

      if (staticContent == null) {
        staticContent = new OStaticContent();
        loadStaticContent(iRequest, iResponse, staticContent);
      }

      if (staticContent.is != null && staticContent.contentSize < 0) {
        ByteArrayOutputStream bytesOutput = new ByteArrayOutputStream();
        GZIPOutputStream stream = new GZIPOutputStream(bytesOutput, 16384);
        try {
          OIOUtils.copyStream(staticContent.is, stream);
          stream.finish();
          byte[] compressedBytes = bytesOutput.toByteArray();
          iResponse.sendStream(
              OHttpUtils.STATUS_OK_CODE,
              OHttpUtils.STATUS_OK_DESCRIPTION,
              staticContent.type,
              new ByteArrayInputStream(compressedBytes),
              compressedBytes.length,
              null,
              new HashMap<String, String>() {
                {
                  put("Content-Encoding", "gzip");
                }
              });
        } finally {
          stream.close();
          bytesOutput.close();
        }
      } else if (staticContent.is != null) {
        iResponse.sendStream(
            OHttpUtils.STATUS_OK_CODE,
            OHttpUtils.STATUS_OK_DESCRIPTION,
            staticContent.type,
            staticContent.is,
            staticContent.contentSize);
      } else {
        iResponse.sendStream(404, "File not found", null, null, 0);
      }

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on loading resource %s", e, iRequest.getUrl());

    } finally {
      if (staticContent != null && staticContent.is != null)
        try {
          staticContent.is.close();
        } catch (IOException e) {
          OLogManager.instance().warn(this, "Error on closing file", e);
        }
    }
    return false;
  }

  public void registerVirtualFolder(final String iName, final OCallable<Object, String> iCallback) {
    virtualFolders.put(iName, iCallback);
  }

  public void unregisterVirtualFolder(final String iName) {
    virtualFolders.remove(iName);
  }

  protected String getResource(final OHttpRequest iRequest) {
    final String url;
    if (OHttpUtils.URL_SEPARATOR.equals(iRequest.getUrl())) url = "/www/index.htm";
    else {
      int pos = iRequest.getUrl().indexOf('?');
      if (pos > -1) url = iRequest.getUrl().substring(0, pos);
      else url = iRequest.getUrl();
    }
    return url;
  }

  protected OStaticContent getVirtualFolderContent(final OHttpRequest iRequest) {
    if (iRequest.getUrl() != null) {
      final int beginPos = iRequest.getUrl().startsWith("/") ? 1 : 0;
      final int endPos = iRequest.getUrl().indexOf("/", beginPos);
      final String firstFolderName =
          endPos > -1
              ? iRequest.getUrl().substring(beginPos, endPos)
              : iRequest.getUrl().substring(beginPos);
      final OCallable<Object, String> virtualFolderCallback = virtualFolders.get(firstFolderName);
      if (virtualFolderCallback != null) {
        // DELEGATE TO THE CALLBACK
        final Object content =
            virtualFolderCallback.call(endPos > -1 ? iRequest.getUrl().substring(endPos + 1) : "");
        if (content == null) return null;

        if (content instanceof OStaticContent) return (OStaticContent) content;
        else if (content instanceof String) {
          final String contentString = (String) content;
          final OStaticContent sc = new OStaticContent();
          sc.is = new ByteArrayInputStream(contentString.getBytes());
          sc.contentSize = contentString.length();
          sc.type = "text/html";
        }
      }
    }
    return null;
  }

  private void loadStaticContent(
      final OHttpRequest iRequest,
      final OHttpResponse iResponse,
      final OStaticContent staticContent)
      throws UnsupportedEncodingException, IOException, FileNotFoundException {
    if (filePath == null && rootPath == null) {
      // GET GLOBAL CONFIG
      rootPath = iRequest.getConfiguration().getValueAsString("orientdb.www.path", "src/site");
      if (rootPath == null) {
        OLogManager.instance()
            .warn(
                this,
                "No path configured. Specify the 'root.path', 'file.path' or the global 'orientdb.www.path' variable",
                rootPath);
        return;
      }
    }

    if (filePath == null) {
      // CHECK DIRECTORY
      final File wwwPathDirectory = new File(rootPath);
      if (!wwwPathDirectory.exists())
        OLogManager.instance()
            .warn(this, "path variable points to '%s' but it doesn't exists", rootPath);
      if (!wwwPathDirectory.isDirectory())
        OLogManager.instance()
            .warn(this, "path variable points to '%s' but it isn't a directory", rootPath);
    }

    String path;
    if (filePath != null)
      // SINGLE FILE
      path = filePath;
    else {
      // GET FROM A DIRECTORY
      final String url = getResource(iRequest);
      if (url.startsWith("/www")) path = rootPath + url.substring("/www".length(), url.length());
      else path = rootPath + url;
    }

    path = URLDecoder.decode(path, "UTF-8");

    if (path.contains("..")) {
      iResponse.sendStream(404, "File not found", null, null, 0);
      return;
    }

    if (server
        .getContextConfiguration()
        .getValueAsBoolean(OGlobalConfiguration.SERVER_CACHE_FILE_STATIC)) {
      final OStaticContentCachedEntry cachedEntry = cacheContents.get(path);
      if (cachedEntry != null) {
        staticContent.is = new ByteArrayInputStream(cachedEntry.content);
        staticContent.contentSize = cachedEntry.size;
        staticContent.type = cachedEntry.type;
      }
    }

    if (staticContent.is == null) {
      File inputFile = new File(path);
      if (!inputFile.exists()) {
        OLogManager.instance().debug(this, "Static resource not found: %s", path);

        iResponse.sendStream(404, "File not found", null, null, 0);
        return;
      }

      if (filePath == null && inputFile.isDirectory()) {
        inputFile = new File(path + "/index.htm");
        if (inputFile.exists()) path = path + "/index.htm";
        else {
          inputFile = new File(path + "/index.html");
          if (inputFile.exists()) path = path + "/index.html";
        }
      }

      staticContent.type = getContentType(path);

      staticContent.is = new BufferedInputStream(new FileInputStream(inputFile));
      staticContent.contentSize = inputFile.length();

      if (server
          .getContextConfiguration()
          .getValueAsBoolean(OGlobalConfiguration.SERVER_CACHE_FILE_STATIC)) {
        // READ THE ENTIRE STREAM AND CACHE IT IN MEMORY
        final byte[] buffer = new byte[(int) staticContent.contentSize];
        for (int i = 0; i < staticContent.contentSize; ++i)
          buffer[i] = (byte) staticContent.is.read();

        OStaticContentCachedEntry cachedEntry = new OStaticContentCachedEntry();
        cachedEntry.content = buffer;
        cachedEntry.size = staticContent.contentSize;
        cachedEntry.type = staticContent.type;

        cacheContents.put(path, cachedEntry);

        staticContent.is = new ByteArrayInputStream(cachedEntry.content);
      }
    }
  }

  public static String getContentType(final String path) {
    if (path.endsWith(".htm") || path.endsWith(".html")) return "text/html";
    else if (path.endsWith(".png")) return "image/png";
    else if (path.endsWith(".jpeg")) return "image/jpeg";
    else if (path.endsWith(".js")) return "application/x-javascript";
    else if (path.endsWith(".css")) return "text/css";
    else if (path.endsWith(".ico")) return "image/x-icon";
    else if (path.endsWith(".otf")) return "font/opentype";

    return "text/plain";
  }
}
