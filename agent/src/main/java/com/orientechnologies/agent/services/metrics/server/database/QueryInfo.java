package com.orientechnologies.agent.services.metrics.server.database;

/** Created by Enrico Risa on 28/09/2018. */
public class QueryInfo {

  private final String language;
  private String statement;
  private long startTime;
  private long elapsedTimeMillis;

  public QueryInfo(String statement, String language, long startTime, long elapsedTimeMillis) {
    this.statement = statement;
    this.language = language;
    this.startTime = startTime;
    this.elapsedTimeMillis = elapsedTimeMillis;
  }

  public String getLanguage() {
    return language;
  }

  public String getStatement() {
    return statement;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getElapsedTimeMillis() {
    return elapsedTimeMillis;
  }
}
