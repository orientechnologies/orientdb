package com.orientechnologies.website.github;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public abstract class GEntity {

  protected ODocument _local;
  private GitHub      owner;

  protected GEntity(GitHub owner, String content) {
    fromJson(content);
    this.owner = owner;
  }

  public void fromJson(String json) {
    _local = new ODocument().fromJSON(json, "noMap");
  }

}
