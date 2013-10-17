package com.geomatys.demo.orientdbOsm;

import java.util.List;

import org.geotoolkit.geometry.jts.JTS;
import org.opengis.geometry.Envelope;

import akka.actor.UntypedActor;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.vividsolutions.jts.io.WKTWriter;

public class TraverseQueryActor extends UntypedActor {
    OGraphDatabase database;

    @Override
    public void preStart() {
	String OSMCountryfilename = "portugal";
	final String tempDirPath = "/Users/christophem/projects/workspaces/graphgis/orientdb-osm/target/";
	final String OrientDbURL = "local:" + tempDirPath + "orientdb-" + OSMCountryfilename;
	database = new OGraphDatabase(OrientDbURL);
	database.open("admin", "admin");
	database.declareIntent(new OIntentMassiveRead());
    }

    @Override
    public void onReceive(Object msg) {
	System.out.println("traverse from " + msg);
	if (msg instanceof String) {

	    String queryString = "TRAVERSE out FROM  " + msg ;
//	    String queryString = "SELECT  FROM  " + msg ;
	    OSQLSynchQuery osmQuery = new OSQLSynchQuery(queryString);
	    List<ODocument> docList = database.query(osmQuery);
	    for (ODocument document : docList) {
		System.out.println(document);
		
		
	    }

	}
    }

    public void postStop() {
	
	database.close();
    }

}
