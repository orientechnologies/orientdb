package com.orientechnologies.orient.core.util;

/**
 * Created by Enrico Risa on 17/11/16.
 */
public class OURLConnection {

    private String url;
    private String type;
    private String path;
    private String dbName;

    public OURLConnection(String url, String type, String path, String dbName) {
        this.url = url;
        this.type = type;
        this.path = path;
        this.dbName = dbName;
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
}
