package com.orientechnologies.orient.core.util;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;

import java.io.File;

/**
 * Created by Enrico Risa on 17/11/16.
 */
public class OURLHelper {

  static public OURLConnection parse(String url) {
    if (url.endsWith("/"))
      url = url.substring(0, url.length() - 1);
    url = url.replace('\\', '/');

    int typeIndex = url.indexOf(':');
    if (typeIndex <= 0)
      throw new OConfigurationException(
          "Error in database URL: the engine was not specified. Syntax is: " + Orient.URL_SYNTAX + ". URL was: " + url);

    String databaseReference = url.substring(typeIndex + 1);
    String type = url.substring(0, typeIndex);
    if (!"remote".equals(type) && !"plocal".equals(type) && !"memory".equals(type))
      throw new OConfigurationException("Error on opening database: the engine '" + type + "' was not found. URL was: " + url
          + ". Registered engines are: [\"memory\",\"remote\",\"plocal\"]");

    int index = databaseReference.lastIndexOf('/');
    String path;
    String dbName;
    String baseUrl;
    if (index > 0) {
      path = databaseReference.substring(0, index);
      dbName = databaseReference.substring(index + 1);
    } else {
      path = "./";
      dbName = databaseReference;
    }
    if ("plocal".equals(type) || "memory".equals(type)) {
      baseUrl = new File(path).getAbsolutePath();
    } else {
      baseUrl = path;
    }
    return new OURLConnection(url, type, baseUrl, dbName);
  }

  static public OURLConnection parseNew(String url) {
    if (url.endsWith("/"))
      url = url.substring(0, url.length() - 1);
    url = url.replace('\\', '/');

    int typeIndex = url.indexOf(':');
    if (typeIndex <= 0)
      throw new OConfigurationException(
          "Error in database URL: the engine was not specified. Syntax is: " + Orient.URL_SYNTAX + ". URL was: " + url);

    String databaseReference = url.substring(typeIndex + 1);
    String type = url.substring(0, typeIndex);
    if ("plocal".equals(type) || "memory".equals(type)) {
      type = "embedded";
    }

    if (!"embedded".equals(type) && !"remote".equals(type))
      throw new OConfigurationException("Error on opening database: the engine '" + type + "' was not found. URL was: " + url
          + ". Registered engines are: [\"embedded\",\"remote\"]");

    int index = databaseReference.lastIndexOf('/');
    String path;
    String dbName;
    String baseUrl;
    if (index > 0) {
      path = databaseReference.substring(0, index);
      dbName = databaseReference.substring(index + 1);
    } else {
      path = "";
      dbName = databaseReference;
    }
    if ("embedded".equals(type) && !path.isEmpty()) {
      baseUrl = new File(path).getAbsolutePath();
    } else {
      baseUrl = path;
    }
    return new OURLConnection(url, type, baseUrl, dbName);
  }
}
