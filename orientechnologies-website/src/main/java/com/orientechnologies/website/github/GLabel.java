package com.orientechnologies.website.github;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GLabel extends GEntity {

  protected GLabel(GitHub github, GEntity owner, String content) {
    super(github, owner, content);
  }

  public String getName() {
    return get("name");
  }

  public String getColor() {
    return get("color");
  }

  @Override
  protected String getBaseUrl() {
    return null;
  }

  public static GLabel fromDoc(ODocument label) {
    return new GLabel(null, null, label.toJSON());
  }
}
