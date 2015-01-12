package com.orientechnologies.website;

import com.orientechnologies.website.interceptor.OrientDBFactoryInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = "com.orientechnologies.website")
public class Application extends WebMvcConfigurerAdapter {

  @Autowired
  private OrientDBFactoryInterceptor interceptor;

  public static void main(final String[] args) throws Exception {

    SpringApplication.run(Application.class, args);

  }



  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(interceptor);
    super.addInterceptors(registry);
  }
}
