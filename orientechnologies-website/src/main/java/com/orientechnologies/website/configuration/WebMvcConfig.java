package com.orientechnologies.website.configuration;

import com.orientechnologies.website.interceptor.OrientDBFactoryInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 21/10/14.
 */

@Configuration
public class WebMvcConfig extends WebMvcConfigurationSupport {

  @Autowired
  private OrientDBFactoryInterceptor interceptor;

  @Override
  public RequestMappingHandlerMapping requestMappingHandlerMapping() {
    ApiVersionRequestMappingHandlerMapping v = new ApiVersionRequestMappingHandlerMapping("api/v");
    v.setInterceptors(getInterceptors());
    return v;
  }

  @Override
  protected void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(interceptor);
  }
}
