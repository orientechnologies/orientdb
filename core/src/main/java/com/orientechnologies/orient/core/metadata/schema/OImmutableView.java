package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Interface for a class implementing a view schema object
 *
 * @author Matan Shukry
 * @since 29/9/2015
 */
public class OImmutableView implements OView {
  private final OImmutableSchema        schema;

  private String                        name;
  private String                        query;
  private ORID                          identity;

  /* Constructors */
  public OImmutableView(OView view, OImmutableSchema schema) {
    this.schema = schema;

    this.name = view.getName();
    this.query = view.getQuery();
    this.identity = view.getDocumentIdentity();
  }

  /* */

  /* */
  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public OView setName(String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getQuery() {
    return this.query;
  }

  @Override
  public OView setQuery(String query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ORID getDocumentIdentity() {
    return this.identity;
  }

  @Override
  public ODocument toStream() {
    throw new UnsupportedOperationException();
  }
}
