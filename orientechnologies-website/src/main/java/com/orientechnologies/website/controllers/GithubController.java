package com.orientechnologies.website.controllers;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.configuration.ApiVersion;
import com.orientechnologies.website.configuration.GitHubConfiguration;
import com.orientechnologies.website.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;
import reactor.core.Reactor;
import reactor.event.Event;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@RestController
@EnableAutoConfiguration
@RequestMapping(value = ApiUrls.GITHUB_V1)
@ApiVersion(1)
public class GithubController {

  @Autowired
  private GitHubConfiguration gitHubConfiguration;

  @Autowired
  private UserService         userService;

  @Autowired
  private Reactor             reactor;

  @RequestMapping(value = ApiUrls.LOGIN, method = RequestMethod.GET)
  public RedirectView login() {

    String baseUrl = gitHubConfiguration.getLoginUrl() + "/authorize?";
    String s = "client_id=" + gitHubConfiguration.getClientId();

    String locationUrl = baseUrl + s + "&scope=user:email,public_repo";
    RedirectView view = new RedirectView();
    view.setUrl(locationUrl);
    return view;
  }

  @RequestMapping(value = ApiUrls.AUTHORIZE, method = RequestMethod.GET, params = { "code" })
  public RedirectView authorize(@RequestParam("code") String code, HttpServletResponse res) {

    String locationUrl = gitHubConfiguration.getLoginUrl() + "/access_token?client_id=" + gitHubConfiguration.getClientId()
        + "&client_secret=" + gitHubConfiguration.getClientSecret() + "&code=" + code;

    RedirectView view = new RedirectView();
    view.setUrl("/");
    try {
      URL obj = new URL(locationUrl);

      HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
      connection.setRequestMethod("POST");
      connection.connect();
      InputStream is = connection.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);

      int numCharsRead;
      char[] charArray = new char[1024];
      StringBuffer sb = new StringBuffer();
      while ((numCharsRead = isr.read(charArray)) > 0) {
        sb.append(charArray, 0, numCharsRead);
      }
      String response = sb.toString();
      String authKey = response.split("&")[0].split("=")[1];
      try {
        userService.initUser(authKey);
        Cookie cookie = new Cookie("prjhub_token", authKey);
        cookie.setMaxAge(2000);
        cookie.setPath("/");
        res.addCookie(cookie);
      } catch (Exception e) {
        view.setUrl("/404");
      }
    } catch (java.io.IOException e) {
      view.setUrl("/500");
    }

    return view;
  }

  @RequestMapping(value = ApiUrls.EVENTS, method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public void handleEvent(HttpServletRequest req) {
    try {

      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      OIOUtils.copyStream(req.getInputStream(), out, -1);
      ODocument doc = new ODocument().fromJSON(out.toString(), "noMap");

      if (doc.containsField("action")) {
        reactor.notify(doc.field("action"), Event.wrap(doc));
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
