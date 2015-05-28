package com.orientechnologies.website.hateoas.assembler;

import com.orientechnologies.website.model.schema.dto.Topic;
import com.orientechnologies.website.model.schema.dto.web.hateoas.TopicResource;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 25/11/14.
 */

@Component
public class TopicAssembler implements ResourceAssembler<Topic, TopicResource> {

  @Override
  public TopicResource toResource(Topic topic) {
    return new TopicResource(topic);
  }
}
