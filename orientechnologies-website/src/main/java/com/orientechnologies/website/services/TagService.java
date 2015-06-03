package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.Tag;

/**
 * Created by Enrico Risa on 03/06/15.
 */
public interface TagService {

  public Tag patchTagByUUID(String organization, String uuid, Tag patch);

  public void dropTag(String organization, String uuid);
}
