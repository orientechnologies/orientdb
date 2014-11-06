package com.orientechnologies.website.repository.impl;

import org.springframework.stereotype.Repository;

import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.OLabel;
import com.orientechnologies.website.repository.LabelRepository;

/**
 * Created by Enrico Risa on 06/11/14.
 */
@Repository
public class LabelRepositoryImpl extends OrientBaseRepository<Label> implements LabelRepository {
  @Override
  public OTypeHolder<Label> getHolder() {
    return OLabel.NAME;
  }

  @Override
  public Class<Label> getEntityClass() {
    return Label.class;
  }
}
