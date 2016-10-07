/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.security.ldap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.security.OSecurityAuthenticator;
import com.orientechnologies.orient.server.security.OSecurityComponent;

import javax.naming.directory.DirContext;
import javax.security.auth.Subject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides an LDAP importer.
 * 
 * @author S. Colin Leister
 * 
 */
public class OLDAPImporter implements OSecurityComponent {
  private final String                              OLDAPUserClass = "_OLDAPUser";

  private boolean                                   _Debug         = false;
  private boolean                                   _Enabled       = true;

  private OServer                                   _Server;

  private int                                       _ImportPeriod  = 60;                                       // Default to 60
                                                                                                               // seconds.
  private Timer                                     _ImportTimer;

  // Used to track what roles are assigned to each database user.
  // Holds a map of the import databases and their corresponding dbGroups.
  private final ConcurrentHashMap<String, Database> _DatabaseMap   = new ConcurrentHashMap<String, Database>();

  // OSecurityComponent
  public void active() {
    // Go through each database entry and check the _OLDAPUsers schema.
    for (Map.Entry<String, Database> dbEntry : _DatabaseMap.entrySet()) {
      Database db = dbEntry.getValue();
      ODatabase<?> odb = null;

      try {
        odb = _Server.getSecurity().openDatabase(db.getName());

        verifySchema(odb);
      } catch (Exception ex) {
        OLogManager.instance().error(this, "OLDAPImporter.active() Database: %s, Exception: %s", db.getName(), ex);
      } finally {
        if (odb != null)
          odb.close();
      }
    }

    ImportTask importTask = new ImportTask();
    _ImportTimer = new Timer(true);
    _ImportTimer.scheduleAtFixedRate(importTask, 30000, _ImportPeriod * 1000); // Wait 30 seconds before starting

    OLogManager.instance().info(this, "**************************************");
    OLogManager.instance().info(this, "** OrientDB LDAP Importer Is Active **");
    OLogManager.instance().info(this, "**************************************");
  }

  // OSecurityComponent
  public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument importDoc) {
    try {
      _Server = oServer;

      _DatabaseMap.clear();

      if (importDoc.containsField("debug")) {
        _Debug = importDoc.field("debug");
      }

      if (importDoc.containsField("enabled")) {
        _Enabled = importDoc.field("enabled");
      }

      if (importDoc.containsField("period")) {
        _ImportPeriod = importDoc.field("period");

        if (_Debug)
          OLogManager.instance().info(this, "Import Period = " + _ImportPeriod);
      }

      if (importDoc.containsField("databases")) {
        List<ODocument> list = importDoc.field("databases");

        for (ODocument dbDoc : list) {
          if (dbDoc.containsField("database")) {
            String dbName = dbDoc.field("database");

            if (_Debug)
              OLogManager.instance().info(this, "config() database: %s", dbName);

            boolean ignoreLocal = true;

            if (dbDoc.containsField("ignoreLocal")) {
              ignoreLocal = dbDoc.field("ignoreLocal");
            }

            if (dbDoc.containsField("domains")) {
              final List<DatabaseDomain> dbDomainsList = new ArrayList<DatabaseDomain>();

              final List<ODocument> dbdList = dbDoc.field("domains");

              for (ODocument dbDomainDoc : dbdList) {
                String domain = null;

                // "domain" is mandatory.
                if (dbDomainDoc.containsField("domain")) {
                  domain = dbDomainDoc.field("domain");

                  // If authenticator is null, it defaults to OLDAPImporter's primary OSecurityAuthenticator.
                  String authenticator = null;

                  if (dbDomainDoc.containsField("authenticator")) {
                    authenticator = dbDomainDoc.field("authenticator");
                  }

                  if (dbDomainDoc.containsField("servers")) {
                    final List<OLDAPServer> ldapServerList = new ArrayList<OLDAPServer>();

                    final List<ODocument> ldapServers = dbDomainDoc.field("servers");

                    for (ODocument ldapServerDoc : ldapServers) {
                      final String url = ldapServerDoc.field("url");

                      boolean isAlias = false;

                      if (ldapServerDoc.containsField("isAlias"))
                        isAlias = ldapServerDoc.field("isAlias");

                      OLDAPServer server = OLDAPServer.validateURL(url, isAlias);

                      if (server != null) {
                        ldapServerList.add(server);
                      } else {
                        OLogManager.instance().error(this, "Import LDAP Invalid server URL for database: %s, domain: %s, URL: %s",
                            dbName, domain, url);
                      }
                    }

                    //
                    final List<User> userList = new ArrayList<User>();

                    final List<ODocument> userDocList = dbDomainDoc.field("users");

                    // userDocList can be null if only the OLDAPUserClass is used instead security.json.
                    if (userDocList != null) {
                      for (ODocument userDoc : userDocList) {
                        if (userDoc.containsField("baseDN") && userDoc.containsField("filter")) {
                          if (userDoc.containsField("roles")) {
                            final String baseDN = userDoc.field("baseDN");
                            final String filter = userDoc.field("filter");

                            if (_Debug)
                              OLogManager.instance().info(this, "config() database: %s, baseDN: %s, filter: %s", dbName, baseDN,
                                  filter);

                            final List<String> roleList = userDoc.field("roles");

                            final User User = new User(baseDN, filter, roleList);

                            userList.add(User);
                          } else {
                            OLogManager.instance().error(this,
                                "Import LDAP The User's \"roles\" property is missing for database %s");
                          }
                        } else {
                          OLogManager.instance().error(this,
                              "Import LDAP The User's \"baseDN\" or \"filter\" property is missing for database %s");
                        }
                      }
                    }

                    DatabaseDomain dbd = new DatabaseDomain(domain, ldapServerList, userList, authenticator);

                    dbDomainsList.add(dbd);
                  } else {
                    OLogManager.instance().error(this, "Import LDAP database %s \"domain\" is missing its \"servers\" property");
                  }
                } else {
                  OLogManager.instance().error(this,
                      "Import LDAP database %s \"domain\" object is missing its \"domain\" property");
                }
              }

              if (dbName != null) {
                Database db = new Database(dbName, ignoreLocal, dbDomainsList);
                _DatabaseMap.put(dbName, db);
              }
            } else {
              OLogManager.instance().error(this, "Import LDAP database %s contains no \"domains\" property");
            }
          } else {
            OLogManager.instance().error(this, "Import LDAP databases contains no \"database\" property");
          }
        }
      } else {
        OLogManager.instance().error(this, "Import LDAP contains no \"databases\" property");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.config() Exception: %s", ex.getMessage());
    }
  }

  // OSecurityComponent
  public void dispose() {
    if (_ImportTimer != null) {
      _ImportTimer.cancel();
      _ImportTimer = null;
    }
  }

  // OSecurityComponent
  public boolean isEnabled() {
    return _Enabled;
  }

  private void verifySchema(ODatabase<?> odb) {
    try {
      System.out.println("calling existsClass odb = " + odb);

      if (!odb.getMetadata().getSchema().existsClass(OLDAPUserClass)) {
        System.out.println("calling createClass");

        OClass ldapUser = odb.getMetadata().getSchema().createClass(OLDAPUserClass);

        System.out.println("calling createProperty");

        OProperty prop = ldapUser.createProperty("Domain", OType.STRING);

        System.out.println("calling setMandatory");

        prop.setMandatory(true);
        prop.setNotNull(true);

        prop = ldapUser.createProperty("BaseDN", OType.STRING);
        prop.setMandatory(true);
        prop.setNotNull(true);

        prop = ldapUser.createProperty("Filter", OType.STRING);
        prop.setMandatory(true);
        prop.setNotNull(true);

        prop = ldapUser.createProperty("Roles", OType.STRING);
        prop.setMandatory(true);
        prop.setNotNull(true);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.verifySchema() Exception: %s", ex.getMessage());
    }
  }

  private class Database {
    private String _Name;

    public String getName() {
      return _Name;
    }

    private boolean _IgnoreLocal;

    public boolean ignoreLocal() {
      return _IgnoreLocal;
    }

    private List<DatabaseDomain> _DatabaseDomains;

    public List<DatabaseDomain> getDatabaseDomains() {
      return _DatabaseDomains;
    }

    public Database(final String name, final boolean ignoreLocal, final List<DatabaseDomain> dbDomains) {
      _Name = name;
      _IgnoreLocal = ignoreLocal;
      _DatabaseDomains = dbDomains;
    }
  }

  private class DatabaseDomain {
    private String _Domain;

    public String getDomain() {
      return _Domain;
    }

    private String            _Authenticator;
    private List<OLDAPServer> _LDAPServers;
    private List<User>        _Users;

    public String getAuthenticator() {
      return _Authenticator;
    }

    public List<OLDAPServer> getLDAPServers() {
      return _LDAPServers;
    }

    public List<User> getUsers() {
      return _Users;
    }

    public DatabaseDomain(final String domain, final List<OLDAPServer> ldapServers, final List<User> userList,
        String authenticator) {
      _Domain = domain;
      _LDAPServers = ldapServers;
      _Users = userList;
      _Authenticator = authenticator;
    }
  }

  private class DatabaseUser {
    private String      _User;
    private Set<String> _Roles = new LinkedHashSet<String>();

    private String getUser() {
      return _User;
    }

    public Set<String> getRoles() {
      return _Roles;
    }

    public void addRoles(Set<String> roles) {
      if (roles != null) {
        for (String role : roles) {
          _Roles.add(role);
        }
      }
    }

    public DatabaseUser(final String user) {
      _User = user;
    }
  }

  private class User {
    private String      _BaseDN;
    private String      _Filter;
    private Set<String> _Roles = new LinkedHashSet<String>();

    public String getBaseDN() {
      return _BaseDN;
    }

    public String getFilter() {
      return _Filter;
    }

    public Set<String> getRoles() {
      return _Roles;
    }

    public User(final String baseDN, final String filter, final List<String> roleList) {
      _BaseDN = baseDN;
      _Filter = filter;

      // Convert the list into a set, for convenience.
      for (String role : roleList) {
        _Roles.add(role);
      }
    }
  }

  // OSecuritySystemAccess
  public Subject getLDAPSubject(final String authName) {
    Subject subject = null;

    OSecurityAuthenticator authMethod = null;

    // If authName is null, use the primary authentication method.
    if (authName == null)
      authMethod = _Server.getSecurity().getPrimaryAuthenticator();
    else
      authMethod = _Server.getSecurity().getAuthenticator(authName);

    if (authMethod != null)
      subject = authMethod.getClientSubject();

    return subject;
  }

  /***
   * LDAP Import
   ***/
  private synchronized void importLDAP() {
    if (_Server.getSecurity() == null) {
      OLogManager.instance().error(this, "OLDAPImporter.importLDAP() ServerSecurity is null");
      return;
    }

    if (_Debug)
      OLogManager.instance().info(this, "OLDAPImporter.importLDAP() \n");

    for (Map.Entry<String, Database> dbEntry : _DatabaseMap.entrySet()) {
      try {
        Database db = dbEntry.getValue();

        ODatabase<?> odb = _Server.getSecurity().openDatabase(db.getName());

        // This set will be filled with all users from the database (unless ignoreLocal is true).
        // As each usersRetrieved list is filled, any matching user will be removed.
        // Once all the DatabaseGroups have been procesed, any remaining users in the set will be deleted from the Database.
        Set<String> usersToBeDeleted = new LinkedHashSet<String>();

        Map<String, DatabaseUser> usersMap = new ConcurrentHashMap<String, DatabaseUser>();

        try {
          // We use this flag to determine whether to proceed with the call to deleteUsers() below.
          // If one or more LDAP servers cannot be reached (perhaps temporarily), we don't want to
          // delete all the database users, locking everyone out until the LDAP server is available again.
          boolean deleteUsers = false;

          // Retrieves all the current OrientDB users from the specified ODatabase and stores them in usersToBeDeleted.
          retrieveAllUsers(odb, db.ignoreLocal(), usersToBeDeleted);

          for (DatabaseDomain dd : db.getDatabaseDomains()) {
            try {
              Subject ldapSubject = getLDAPSubject(dd.getAuthenticator());

              if (ldapSubject != null) {
                DirContext dc = OLDAPLibrary.openContext(ldapSubject, dd.getLDAPServers(), _Debug);

                if (dc != null) {
                  deleteUsers = true;

                  try {
                    // Combine the "users" from security.json's "ldapImporter" and the class OLDAPUserClass.
                    List<User> userList = new ArrayList<User>();
                    userList.addAll(dd.getUsers());

                    retrieveLDAPUsers(odb, dd.getDomain(), userList);

                    for (User user : userList) {
                      List<String> usersRetrieved = new ArrayList<String>();

                      OLogManager.instance().info(this,
                          "OLDAPImporter.importLDAP() Calling retrieveUsers for Database: %s, Filter: %s", db.getName(),
                          user.getFilter());

                      OLDAPLibrary.retrieveUsers(dc, user.getBaseDN(), user.getFilter(), usersRetrieved, _Debug);

                      if (!usersRetrieved.isEmpty()) {
                        for (String upn : usersRetrieved) {
                          if (usersToBeDeleted.contains(upn))
                            usersToBeDeleted.remove(upn);

                          OLogManager.instance().info(this, "OLDAPImporter.importLDAP() Database: %s, Filter: %s, UPN: %s",
                              db.getName(), user.getFilter(), upn);

                          DatabaseUser dbUser = null;

                          if (usersMap.containsKey(upn))
                            dbUser = usersMap.get(upn);
                          else {
                            dbUser = new DatabaseUser(upn);
                            usersMap.put(upn, dbUser);
                          }

                          if (dbUser != null) {
                            dbUser.addRoles(user.getRoles());
                          }
                        }
                      } else {
                        OLogManager.instance().info(this,
                            "OLDAPImporter.importLDAP() No users found at BaseDN: %s, Filter: %s, for Database: %s",
                            user.getBaseDN(), user.getFilter(), db.getName());
                      }
                    }
                  } finally {
                    dc.close();
                  }
                } else {
                  OLogManager.instance().error(this,
                      "OLDAPImporter.importLDAP() Could not obtain an LDAP DirContext for Database %s", db.getName());
                }
              } else {
                OLogManager.instance().error(this, "OLDAPImporter.importLDAP() Could not obtain an LDAP Subject for Database %s",
                    db.getName());
              }
            } catch (Exception ex) {
              OLogManager.instance().error(this, "OLDAPImporter.importLDAP() Database: %s, Exception: %s", db.getName(), ex);
            }
          }

          // Imports the LDAP users into the specified database, if it exists.
          importUsers(odb, usersMap);

          if (deleteUsers)
            deleteUsers(odb, usersToBeDeleted);
        } finally {
          if (usersMap != null)
            usersMap.clear();
          if (usersToBeDeleted != null)
            usersToBeDeleted.clear();
          if (odb != null)
            odb.close();
        }
      } catch (Exception ex) {
        OLogManager.instance().error(this, "OLDAPImporter.importLDAP() Exception: %s", ex.getMessage());
      }
    }
  }

  // Loads the User object from the OLDAPUserClass class for each domain.
  // This is equivalent to the "users" objects in "ldapImporter" of security.json.
  private void retrieveLDAPUsers(final ODatabase<?> odb, final String domain, final List<User> userList) {
    try {
      String sql = String.format("SELECT FROM %s WHERE Domain = \"%s\"", OLDAPUserClass, domain);

      List<ODocument> users = new OSQLSynchQuery<ODocument>(sql).run();

      for (ODocument userDoc : users) {
        String roles = userDoc.field("Roles");

        if (roles != null) {
          List<String> roleList = new ArrayList<String>();

          String[] roleArray = roles.split(",");

          for (String role : roleArray) {
            roleList.add(role.trim());
          }

          User user = new User((String) userDoc.field("BaseDN"), (String) userDoc.field("Filter"), roleList);
          userList.add(user);
        } else {
          OLogManager.instance().error(this,
              "OLDAPImporter.retrieveLDAPUsers() Roles is missing for entry Database: %s, Domain: %s", odb.getName(), domain);
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.retrieveLDAPUsers() Database: %s, Domain: %s, Exception: %s", odb.getName(),
          domain, ex);
    }
  }

  private void retrieveAllUsers(final ODatabase<?> odb, final boolean ignoreLocal, final Set<String> usersToBeDeleted) {
    try {
      String sql = "SELECT FROM OUser";

      if (ignoreLocal)
        sql = "SELECT FROM OUser WHERE _externalUser = true";

      List<ODocument> users = new OSQLSynchQuery<ODocument>(sql).run();

      for (ODocument user : users) {
        String name = user.field("name");

        if (name != null) {
          if (!(name.equals("admin") || name.equals("reader") || name.equals("writer"))) {
            usersToBeDeleted.add(name);

            OLogManager.instance().info(this, "OLDAPImporter.retrieveAllUsers() Database: %s, User: %s", odb.getName(), name);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.retrieveAllUsers() Database: %s, Exception: %s", odb.getName(), ex);
    }
  }

  private void deleteUsers(final ODatabase<?> odb, final Set<String> usersToBeDeleted) {
    try {
      for (String user : usersToBeDeleted) {
        odb.command(new OCommandSQL("DELETE FROM OUser WHERE name = ?")).execute(user);

        OLogManager.instance().info(this, "OLDAPImporter.deleteUsers() Deleted User: %s from Database: %s", user, odb.getName());
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.deleteUsers() Database: %s, Exception: %s", odb.getName(), ex.getMessage());
    }
  }

  private void importUsers(final ODatabase<?> odb, final Map<String, DatabaseUser> usersMap) {
    try {
      for (Map.Entry<String, DatabaseUser> entry : usersMap.entrySet()) {
        String upn = entry.getKey();

        if (upsertDbUser(odb, upn, entry.getValue().getRoles()))
          OLogManager.instance().info(this, "Added/Modified Database User %s in Database %s", upn, odb.getName());
        else
          OLogManager.instance().error(this, "Failed to add/update Database User %s in Database %s", upn, odb.getName());
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.importUsers() Database: %s, Exception: %s", odb.getName(), ex.getMessage());
    }
  }

  /*
   * private boolean dbUserExists(ODatabase<?> db, String upn) { try { List<ODocument> list = new OSQLSynchQuery<ODocument>(
   * "SELECT FROM OUser WHERE name = ?").run(upn);
   * 
   * return !list.isEmpty(); } catch(Exception ex) { OLogManager.instance().debug(this, "dbUserExists() Exception: ", ex); }
   * 
   * return true; // Better to not add a user than to overwrite one. }
   */
  private boolean upsertDbUser(ODatabase<?> db, String upn, Set<String> roles) {
    try {
      // Create a random password to set for each imported user in case allowDefault is set to true.
      // We don't want blank or simple passwords set on the imported users, just in case.
      // final String password = OSecurityManager.instance().createSHA256(String.valueOf(new java.util.Random().nextLong()));

      final String password = UUID.randomUUID().toString();

      StringBuilder sb = new StringBuilder();
      sb.append(
          "UPDATE OUser SET name = ?, password = ?, status = \"ACTIVE\", _externalUser = true, roles = (SELECT FROM ORole WHERE name in [");

      String[] roleParams = new String[roles.size()];

      Iterator<String> it = roles.iterator();

      int cnt = 0;

      while (it.hasNext()) {
        String role = it.next();

        sb.append("'");
        sb.append(role);
        sb.append("'");

        if (it.hasNext())
          sb.append(", ");

        roleParams[cnt] = role;

        cnt++;
      }

      sb.append("]) UPSERT WHERE name = ?");

      // db.command(new OCommandSQL(sb.toString())).execute(upn, password, roleParams, upn);
      db.command(new OCommandSQL(sb.toString())).execute(upn, password, upn);

      return true;
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OLDAPImporter.upsertDbUser() Exception: %s", ex.getMessage());
    }

    return false;
  }

  private class ImportTask extends TimerTask {
    @Override
    public void run() {
      importLDAP();
    }
  }
}
