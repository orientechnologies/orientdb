package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.model.schema.dto.Tag;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.TagRepository;
import com.orientechnologies.website.services.TagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by Enrico Risa on 03/06/15.
 */
@Service
public class TagServiceImpl implements TagService {

  @Autowired
  private OrganizationRepository orgRepository;

  @Autowired
  private TagRepository          tagRepository;

  @Transactional
  @Override
  public Tag patchTagByUUID(String organization, String uuid, Tag patch) {

    Tag t = orgRepository.findTagByUUID(organization, uuid);
    t = tagRepository.patch(t, patch);
    return t;
  }

  @Transactional
  @Override
  public void dropTag(String organization, String uuid) {

    Tag t = orgRepository.findTagByUUID(organization, uuid);
    tagRepository.delete(t);
  }
}
