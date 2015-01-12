package com.orientechnologies.website.configuration;

import com.orientechnologies.website.interceptor.OrientDBFactoryInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * Created by Enrico Risa on 21/10/14.
 */

//@Configuration
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
