package com.orientechnologies.orient.core.metadata.schema;

public class OImmutableView extends OImmutableClass implements OView {

  String query;

  public OImmutableView(OView oClass, OImmutableSchema schema) {
    super(oClass, schema);
    this.query = oClass.getQuery();
  }

  @Override
  public String getQuery() {
    return query;
  }
}
