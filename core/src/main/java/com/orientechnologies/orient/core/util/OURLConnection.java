package com.orientechnologies.orient.core.util;

import com.orientechnologies.orient.core.db.ODatabaseType;
import java.util.Optional;

/** Created by Enrico Risa on 17/11/16. */
public class OURLConnection {

  private String url;
  private String type;
  private String path;
  private String dbName;
  private Optional<ODatabaseType> dbType;

  public OURLConnection(String url, String type, String path, String dbName) {
    this(url, type, path, dbName, Optional.empty());
  }

  public OURLConnection(
      String url, String type, String path, String dbName, Optional<ODatabaseType> dbType) {
    this.url = url;
    this.type = type;
    this.path = path;
    this.dbName = dbName;
    this.dbType = dbType;
  }

  public String getDbName() {
    return dbName;
  }

  public String getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public String getUrl() {
    return url;
  }

  public Optional<ODatabaseType> getDbType() {
    return dbType;
  }

  @Override
  public String toString() {
    return "OURLConnection{"
        + "url='"
        + url
        + "', type='"
        + type
        + "', path='"
        + path
        + "', dbName='"
        + dbName
        + "', dbType="
        + dbType
        + '}';
  }
}
