package com.orientechnologies.website.security;

/**
 * Created by Enrico Risa on 04/06/15.
 */
public class Permissions {

  public static final String ISSUE_LABEL       = "hasPermission(#user,#owner,'issue.label')";

  public static final String ORG_MEMBER        = "hasPermission(#user,#name,'client.member')";

  public static final String ISSUE_ATTACHMENTS = "hasPermission(#user,#owner,'issue.attachments')";

}
