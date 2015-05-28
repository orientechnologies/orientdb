package com.orientechnologies.website.model.schema.dto.web.hateoas;

import com.orientechnologies.website.model.schema.dto.Topic;
import org.springframework.hateoas.Resource;

/**
 * Created by Enrico Risa on 28/05/15.
 */
public class TopicResource extends Resource<Topic> {

  public TopicResource(Topic content) {
    super(content);
  }
}
