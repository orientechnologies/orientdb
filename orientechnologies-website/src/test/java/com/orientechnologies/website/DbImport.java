package com.orientechnologies.website;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

import java.io.IOException;

/**
 * Created by Enrico Risa on 23/04/15.
 */
public class DbImport {

  public static void main(String[] args) {

    OrientGraph graph = new OrientGraph("plocal:databases/odbsiteNew");
    ODatabaseImport oDatabaseImport = null;
    try {
      oDatabaseImport = new ODatabaseImport(graph.getRawGraph(),
          "/Users/enricorisa/Coding/OrientTech/orientdb-enterprise/orientechnologies-website/odbsite.json",
          new OCommandOutputListener() {
            @Override
            public void onMessage(String iText) {
              System.out.println(iText);
            }
          });

      oDatabaseImport.importDatabase();
      oDatabaseImport.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
