package com.orientechnologies.workbench;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;

public final class OWorkbenchUtils {

  public static String fetchFromRemoteServer(final ODocument server, final URL iRemoteUrl) throws IOException {
    return fetchFromRemoteServer(server, iRemoteUrl, "GET");
  }

  public static String fetchFromRemoteServer(final ODocument server, final URL iRemoteUrl, final String iMethod) throws IOException {

    HttpURLConnection urlConnection = openConnectionForServer(server, iRemoteUrl, iMethod);

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

  public static InputStream fetchInputRemoteServer(final ODocument server, final URL iRemoteUrl) throws IOException {
    return fetchInputRemoteServer(server, iRemoteUrl, "GET");
  }

  public static InputStream fetchInputRemoteServer(final ODocument server, final URL iRemoteUrl, String iMethod) throws IOException {
    HttpURLConnection urlConnection = openConnectionForServer(server, iRemoteUrl, iMethod);
    InputStream is = urlConnection.getInputStream();

    return is;
  }

  private static HttpURLConnection openConnectionForServer(final ODocument server, final URL iRemoteUrl, final String iMethod)
      throws IOException, ProtocolException {
    HttpURLConnection urlConnection = (HttpURLConnection) iRemoteUrl.openConnection();

    String enc = server.field("password");
    if (enc == null) {
      throw new IOException("401");
    }
    String pwd = null;
    try {
      pwd = OL.decrypt(enc);
    } catch (Exception e) {
//      e.printStackTrace();
      pwd = enc;
    }
    String authString = server.field("user") + ":" + pwd;
    String authStringEnc = OBase64Utils.encodeBytes(authString.getBytes());
    urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);

    urlConnection.setRequestMethod(iMethod);

    urlConnection.connect();
    return urlConnection;
  }

  public static String sendToRemoteServer(final ODocument server, final URL iRemoteUrl, final String iMethod, final String body)
      throws IOException {

    HttpURLConnection urlConnection = (HttpURLConnection) iRemoteUrl.openConnection();

    String enc = server.field("password");
    if (enc == null) {
      throw new IOException("401");
    }
    String pwd = null;
    try {
      pwd = OL.decrypt(enc);
    } catch (Exception e) {
      e.printStackTrace();
    }
    String authString = server.field("user") + ":" + pwd;
    String authStringEnc = OBase64Utils.encodeBytes(authString.getBytes());
    urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);

    urlConnection.setRequestProperty("content-type", "text/plain; charset=utf-8");
    urlConnection.setDoOutput(true);
    urlConnection.setRequestMethod(iMethod);
    urlConnection.connect();
    OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
    out.write(body);
    out.close();
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