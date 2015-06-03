package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Tag;
import com.orientechnologies.website.repository.impl.OrientBaseAutoRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 03/06/15.
 */
@Repository
public class TagRepositoryImpl extends OrientBaseAutoRepository<Tag> implements TagRepository {
  @Override
  public Class<Tag> getEntityClass() {
    return Tag.class;
  }

  @Override
  public Tag patch(Tag entity, Tag patch) {

    if (patch.getName() != null) {
      entity.setName(patch.getName());
    }
    if (patch.getColor() != null) {
      entity.setColor(patch.getColor());
    }
    return save(entity);
  }
}
