package com.orientechnologies.orient.core.sql.parser;

import org.junit.Test;

public class OAlterClassStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER CLASS Foo NAME Bar");
    checkRightSyntax("alter class Foo name Bar");
    checkRightSyntax("ALTER CLASS Foo NAME Bar UNSAFE");
    checkRightSyntax("alter class Foo name Bar unsafe");

    checkRightSyntax("ALTER CLASS `Foo bar` NAME `Bar bar`");

    checkRightSyntax("ALTER CLASS Foo SHORTNAME Bar");
    checkRightSyntax("ALTER CLASS Foo shortname Bar");

    checkRightSyntax("ALTER CLASS Foo ADDCLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo addcluster bar");

    checkRightSyntax("ALTER CLASS Foo REMOVECLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo removecluster bar");

    checkRightSyntax("ALTER CLASS Foo DESCRIPTION bar");
    checkRightSyntax("ALTER CLASS Foo description bar");

    checkRightSyntax("ALTER CLASS Foo ENCRYPTION des");
    checkRightSyntax("ALTER CLASS Foo encryption des");

    checkRightSyntax("ALTER CLASS Foo CLUSTERSELECTION default");

    checkRightSyntax("ALTER CLASS Foo CLUSTERSELECTION round-robin");
    checkRightSyntax("ALTER CLASS Foo clusterselection round-robin");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASS Bar");
    checkRightSyntax("ALTER CLASS Foo superclass Bar");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASS +Bar");
    checkRightSyntax("ALTER CLASS Foo SUPERCLASS -Bar");
    checkRightSyntax("ALTER CLASS Foo superclass null");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES Bar");
    checkRightSyntax("ALTER CLASS Foo superclasses Bar");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES Bar, Bazz, braz");
    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES Bar,Bazz,braz");
    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES null");

    checkRightSyntax("ALTER CLASS Foo OVERSIZE 2");
    checkRightSyntax("ALTER CLASS Foo oversize 2");
    checkRightSyntax("ALTER CLASS Foo OVERSIZE 1.5");

    checkRightSyntax("ALTER CLASS Foo STRICTMODE true");
    checkRightSyntax("ALTER CLASS Foo strictmode true");
    checkRightSyntax("ALTER CLASS Foo STRICTMODE false");

    checkRightSyntax("ALTER CLASS Foo ADDCLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo addcluster bar");

    checkRightSyntax("ALTER CLASS Foo REMOVECLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo removecluster bar");

    checkRightSyntax("ALTER CLASS Foo CUSTOM bar=baz");
    checkRightSyntax("ALTER CLASS Foo custom bar=baz");
    checkRightSyntax("ALTER CLASS Foo CUSTOM bar = baz");

    checkRightSyntax("alter class polymorpicIdsPropagation removecluster 436");

    checkRightSyntax("ALTER CLASS Person CUSTOM `onCreate.identityType`=role");

    checkRightSyntax("ALTER CLASS Person DEFAULTCLUSTER foo");
    checkRightSyntax("ALTER CLASS Person DEFAULTCLUSTER 15");

    checkWrongSyntax("ALTER CLASS Foo NAME Bar baz");

    checkWrongSyntax("ALTER CLASS Foo SUPERCLASS *Bar");
    checkWrongSyntax("ALTER CLASS Foo oversize 1.1.1");
    checkWrongSyntax("ALTER CLASS Foo oversize bar");
  }
}
