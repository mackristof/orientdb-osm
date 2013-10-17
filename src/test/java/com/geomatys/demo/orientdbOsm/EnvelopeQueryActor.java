package com.geomatys.demo.orientdbOsm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.geotoolkit.geometry.jts.JTS;
import org.opengis.geometry.Envelope;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.vividsolutions.jts.io.WKTWriter;

public class EnvelopeQueryActor extends UntypedActor {
    OGraphDatabase database;
    List<String> treadedEdges = new ArrayList<String>();

    @Override
    public void preStart() {
	String OSMCountryfilename = "portugal";
	final String tempDirPath = "/Users/christophem/projects/workspaces/graphgis/orientdb-osm/target/";
	final String OrientDbURL = "local:" + tempDirPath + "orientdb-" + OSMCountryfilename;
	database = new OGraphDatabase(OrientDbURL);
	database.open("admin", "admin");
	database.declareIntent(new OIntentMassiveRead());
	OGlobalConfiguration.TX_USE_LOG.setValue(false);

    }
    @Override
    public void onReceive(Object msg) {
	if (msg instanceof Envelope) {
	    String queryString = "SELECT  FROM OSM_NODE WHERE  @rid > ? LIMIT 10000";
	    OSQLSynchQuery osmQuery = new OSQLSynchQuery(queryString);
	    ORID last = new ORecordId();
	    long startTime = System.currentTimeMillis();
	    List<ODocument> docList = database.query(osmQuery, last);
	    System.out.println("duration=" + ((System.currentTimeMillis() - startTime) / 1000));
	    int cpt = 0;
	    while (!docList.isEmpty()) {
		System.out.println(docList.size()+" x "+(cpt++));
		for (ODocument oDocument : docList) {
		    treatOsmNode(oDocument);
		    last = oDocument.getIdentity();
		}
		startTime = System.currentTimeMillis();
		docList = database.query(osmQuery, last);
		System.out.println("duration=" + ((System.currentTimeMillis() - startTime) / 1000));
	    }
	}

    }

    private void treatOsmNode(ODocument document) {

	if (document.field("in") != null && document.field("in") instanceof OMVRBTreeRIDSet) {

	    Iterator<OIdentifiable> iterInSet = ((OMVRBTreeRIDSet) document.field("in")).iterator();
	    while (iterInSet.hasNext()) {
		OIdentifiable oIdentifiable = (OIdentifiable) iterInSet.next();
		ODocument inDoc = (ODocument) oIdentifiable.getRecord();

		if (!treadedEdges.contains(((ODocument) inDoc.field("out")).getIdentity().toString()) && ((ODocument) inDoc.field("out")).field("@Class").equals("OSM_WAY")) {
		     ActorRef actorRef = getContext().actorOf(new Props(TraverseQueryActor.class));
		     actorRef.tell(((ODocument)inDoc.field("out")).getIdentity().toString(), getSelf());
		    treadedEdges.add(((ODocument) inDoc.field("out")).getIdentity().toString());
		}

	    }
	}
    }

    public void postStop() {
	database.close();
    }

}
