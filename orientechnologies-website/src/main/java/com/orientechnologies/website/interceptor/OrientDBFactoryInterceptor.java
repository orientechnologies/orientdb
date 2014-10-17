package com.orientechnologies.website.interceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.website.OrientDBFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

@Component
public class OrientDBFactoryInterceptor extends HandlerInterceptorAdapter {

  @Autowired
  private OrientDBFactory factory;

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
    OrientGraph graph = factory.getGraph();
    ODatabaseRecordThreadLocal.INSTANCE.set(graph.getRawGraph());
    return super.preHandle(request, response, handler);
  }

  @Override
  public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView)
      throws Exception {

    factory.unsetDb();
    ODatabaseRecordThreadLocal.INSTANCE.set(null);
    super.postHandle(request, response, handler, modelAndView);
  }
}
