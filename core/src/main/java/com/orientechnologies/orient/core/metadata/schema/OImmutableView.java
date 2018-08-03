package com.orientechnologies.orient.core.metadata.schema;

public class OImmutableView extends OImmutableClass implements OView {

  private final int updateIntervalSeconds;
  String query;

  public OImmutableView(OView view, OImmutableSchema schema) {
    super(view, schema);
    this.query = view.getQuery();
    this.updateIntervalSeconds = view.getUpdateIntervalSeconds();
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public int getUpdateIntervalSeconds() {
    return updateIntervalSeconds;
  }
}
