package com.geomatys.demo.orientdbOsm;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

@RunWith(JUnit4.class)
public class ImportTest {
    private static final String tempDirPath = "./target/";
    List<String> treadedEdges = new ArrayList<String>();

    @Test
    public void testImportMalta() throws Exception {
        importCountry("malta");
    }

    @Test
    public void testImportPortugal() throws Exception {
        importCountry("portugal");
    }


    

    private void importCountry(String country) throws Exception {
        if (!new File(tempDirPath+country+"-latest.osm.bz2").exists()) {
            URL website = new URL("http://download.geofabrik.de/europe/"+country+"-latest.osm.bz2");
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream(tempDirPath+country+"-latest.osm.bz2");
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            fos.close();
        }
        assert new File(tempDirPath+country+"-latest.osm.bz2").exists();
        unBZ2(new File(tempDirPath+country+"-latest.osm.bz2"),new File(tempDirPath+country+"-latest.osm"));
        assert new File(tempDirPath+country+"-latest.osm").exists();
        OSMOrientdbImport app = new OSMOrientdbImport();
        OSMOrientdbImport.statOSM(new File(tempDirPath+country+"-latest.osm").getAbsolutePath());
        String OrientdbURL = "local:"+tempDirPath+"orientdb-"+country;
        OGraphDatabase database = new OGraphDatabase(OrientdbURL);
        OSMOrientdbImport.resetDatabase(OrientdbURL);
        database.open("admin", "admin");
        OSMOrientdbImport.writeOSM(database, new File(tempDirPath + country + "-latest.osm").getAbsolutePath());
        database.open("admin", "admin");
        assert database.countVertexes()>0;
//        OClass oSMNodeClass = database.getMetadata().getSchema().getClass("OSM_NODE");
//        OProperty oProperty = oSMNodeClass.getProperty("geom");
//        assert oProperty.getAllIndexes().size()>0;
        database.close();

    }

    private static void unBZ2(final File inputFile, final File outputDir) throws FileNotFoundException, IOException {
        FileInputStream fin = new FileInputStream(inputFile);
        BufferedInputStream in = new BufferedInputStream(fin);
        FileOutputStream out = new FileOutputStream(outputDir);
        BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
        final byte[] buffer = new byte[1024];
        int n = 0;
        while (-1 != (n = bzIn.read(buffer))) {
            out.write(buffer, 0, n);
        }
        out.close();
        bzIn.close();

    }

   

    @Test
    public void readFullAsynchPortugal() throws Exception {
	readFullAsynch("portugal");
    }


    public void readFullAsynch(String country) throws Exception {
        String OrientdbURL = "local:"+tempDirPath+"orientdb-"+country;
        OGraphDatabase database = new OGraphDatabase(OrientdbURL);
        database.open("admin", "admin");

        final String queryString = "SELECT * FROM OSM_NODE WHERE geom is not null";
        long startTime = System.currentTimeMillis();
        database.command(
                new OSQLAsynchQuery<>(queryString,
                        new OCommandResultListener() {
                            int resultCount = 0;

                            @Override
                            public boolean result(Object iRecord) {
                                System.out.println(resultCount++);
                                //Vertex doc = database.getVertex(iRecord);
                                return true;
                            }

                            @Override
                            public void end() {
                                //To change body of implemented methods use File | Settings | File Templates.
                            }
                        })).execute();

    }
    
    
    @Test
    public void readPaginedSynchPortugal() throws Exception {
	readPaginedSynch("portugal");
    }


    public void readPaginedSynch(String country) throws Exception {
	String OrientdbURL = "local:" + tempDirPath + "orientdb-" + country;
	OGraphDatabase database = new OGraphDatabase(OrientdbURL);
	database.open("admin", "admin");

	String queryString = "SELECT  FROM OSM_NODE WHERE  @rid > ? LIMIT 10000";
	OSQLSynchQuery osmQuery = new OSQLSynchQuery(queryString);
	ORID last = new ORecordId();
	long startTime = System.currentTimeMillis();
	List<ODocument> docList = database.query(osmQuery, last);
	System.out.println("duration=" + ((System.currentTimeMillis() - startTime) / 1000));
	int cpt = 0;
	while (!docList.isEmpty()) {
	    System.out.println(docList.size() + " x " + (cpt++));
	    for (ODocument oDocument : docList) {
		treatOsmNode(oDocument);
		last = oDocument.getIdentity();
	    }
	    startTime = System.currentTimeMillis();
	    docList = database.query(osmQuery, last);
	    System.out.println("duration=" + ((System.currentTimeMillis() - startTime) / 1000));
	}

    }
    
    private void treatOsmNode(ODocument document) {

   	if (document.field("in") != null && document.field("in") instanceof OMVRBTreeRIDSet) {

   	    Iterator<OIdentifiable> iterInSet = ((OMVRBTreeRIDSet) document.field("in")).iterator();
   	    while (iterInSet.hasNext()) {
   		OIdentifiable oIdentifiable = (OIdentifiable) iterInSet.next();
   		ODocument inDoc = (ODocument) oIdentifiable.getRecord();

//   		if (!treadedEdges.contains(((ODocument) inDoc.field("out")).getIdentity().toString()) && ((ODocument) inDoc.field("out")).field("@Class").equals("OSM_WAY")) {
//
//   		    treadedEdges.add(((ODocument) inDoc.field("out")).getIdentity().toString());
//   		}

   	    }
   	}
       }
    
    
    
}
