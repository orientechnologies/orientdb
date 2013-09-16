package com.orientechnologies.orient.monitor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

public final class OMonitorUtils {

  public static String fetchFromRemoteServer(final ODocument server, final URL iRemoteUrl) throws IOException {
    return fetchFromRemoteServer(server, iRemoteUrl, "GET");
  }

  public static String fetchFromRemoteServer(final ODocument server, final URL iRemoteUrl, final String iMethod) throws IOException {

    HttpURLConnection urlConnection = (HttpURLConnection) iRemoteUrl.openConnection();

    String authString = server.field("user") + ":" + server.field("password");
    String authStringEnc = OBase64Utils.encodeBytes(authString.getBytes());
    urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);

    urlConnection.setRequestMethod(iMethod);

    urlConnection.connect();

    InputStream is = urlConnection.getInputStream();
    InputStreamReader isr = new InputStreamReader(is);

    int numCharsRead;
    char[] charArray = new char[1024];
    StringBuffer sb = new StringBuffer();
    while ((numCharsRead = isr.read(charArray)) > 0) {
      sb.append(charArray, 0, numCharsRead);
    }
    return sb.toString();
  }
}