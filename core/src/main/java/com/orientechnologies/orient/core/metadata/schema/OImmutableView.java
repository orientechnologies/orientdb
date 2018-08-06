package com.orientechnologies.orient.core.metadata.schema;

import java.util.ArrayList;
import java.util.List;

public class OImmutableView extends OImmutableClass implements OView {

  private final int          updateIntervalSeconds;
  private final List<String> watchClasses;
  String query;

  public OImmutableView(OView view, OImmutableSchema schema) {
    super(view, schema);
    this.query = view.getQuery();
    this.updateIntervalSeconds = view.getUpdateIntervalSeconds();
    this.watchClasses = view.getWatchClasses() == null ? null : new ArrayList<>(view.getWatchClasses());
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public int getUpdateIntervalSeconds() {
    return updateIntervalSeconds;
  }

  @Override
  public List<String> getWatchClasses() {
    return watchClasses;
  }
}
