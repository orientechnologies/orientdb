package com.orientechnologies.website.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * Created by Enrico Risa on 21/10/14.
 */

@Configuration
public class WebMvcConfig extends WebMvcConfigurationSupport {
  @Override
  public RequestMappingHandlerMapping requestMappingHandlerMapping() {
    return new ApiVersionRequestMappingHandlerMapping("v");
  }
}
