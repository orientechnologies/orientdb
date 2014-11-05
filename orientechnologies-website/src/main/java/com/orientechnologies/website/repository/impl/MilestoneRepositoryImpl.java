package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OMilestone;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Milestone;
import com.orientechnologies.website.repository.MilestoneRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 04/11/14.
 */
@Repository
public class MilestoneRepositoryImpl extends OrientBaseRepository<Milestone> implements MilestoneRepository {
  @Override
  public OTypeHolder<Milestone> getHolder() {
    return OMilestone.NUMBER;
  }

  @Override
  public Class<Milestone> getEntityClass() {
    return Milestone.class;
  }
}
