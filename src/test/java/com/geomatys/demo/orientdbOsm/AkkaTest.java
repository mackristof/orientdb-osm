package com.geomatys.demo.orientdbOsm;

import org.geotoolkit.referencing.CRS;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.util.FactoryException;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.ActorRef;

@RunWith(JUnit4.class)
public class AkkaTest {
    

    @Test
    public void testBasicQueryActor() throws NoSuchAuthorityCodeException, FactoryException {

	final ActorSystem actorSystem = ActorSystem.create("MySystem");
	ActorRef actorRef = actorSystem.actorOf(new Props(EnvelopeQueryActor.class), "EnvelopeQueryActor");
	actorRef.tell(CRS.getEnvelope(CRS.decode("EPSG:4326")), null);
	try {
		
		while (!actorRef.isTerminated()){
		    Thread.sleep(1000);
		}
	} catch (Exception e) {
	    e.printStackTrace();
	}

	actorSystem.stop(actorRef);
	actorSystem.shutdown();

    }

}
