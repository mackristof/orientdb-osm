package com.geomatys.demo.orientdbOsm;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.log4j.Logger;
import org.geotoolkit.data.osm.model.*;
import org.geotoolkit.data.osm.xml.OSMXMLReader;
//import org.geotoolkit.orientdb.spatial.OrientSpatialUtilities;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

import java.io.File;
import java.util.List;

/**
 * Hello world!
 */
public class OSMOrientdbImport {
    static String OSMCountryfilename = "malta";
    private static final String tempDirPath = "./target/";

    private static final String OrientDbURL = "local:"+tempDirPath+"orientdb-"+ OSMCountryfilename;
    static OGraphDatabase database = new OGraphDatabase(OrientDbURL);

    // private final String OrientDbURL = "remote:127.0.0.1/db-" +
    // OSMCountryfilename;

    static String pathnameOsmFile = System.getProperty("user.home") + "/data/orient/" + OSMCountryfilename + "-latest.osm";
    static Logger log = Logger.getLogger(OSMOrientdbImport.class);

    public static void main(String[] args) throws Exception {

        OSMOrientdbImport app = new OSMOrientdbImport();
        app.statOSM(pathnameOsmFile);
        database = resetDatabase(OrientDbURL);
        app.writeOSM(database,pathnameOsmFile);
        app.database.close();

    }

    public static void statOSM(String pathnameOsmFile) throws Exception {
        final OSMXMLReader reader = new OSMXMLReader();
        try {
            int nbNode = 0;
            int nbWay = 0;
            int nbRelation = 0;
            log.trace(new File(pathnameOsmFile).getAbsolutePath());
            reader.setInput(new File(pathnameOsmFile));

            while (reader.hasNext()) {
                final Object obj = reader.next();

                if (obj instanceof Node) {
                    nbNode++;
                } else if (obj instanceof Way) {
                    nbWay++;
                } else if (obj instanceof Relation) {
                    nbRelation++;
                }
            }
            log.info("nbnode=" + nbNode + " , nbway" + nbWay + " , nbrelation" + nbRelation);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            reader.dispose();

        }


    }

    public static void writeOSM(OGraphDatabase database, String pathnameOsmFile) throws Exception {



        OClass oSMNodeClass = database.getMetadata().getSchema().createClass("OSM_NODE", database.getMetadata().getSchema().getClass("V"));

        OClass oSMWayClass = database.getMetadata().getSchema().createClass("OSM_WAY", database.getMetadata().getSchema().getClass("V"));
        OClass oSMRelationClass = database.getMetadata().getSchema().createClass("OSM_RELATION", database.getMetadata().getSchema().getClass("V"));

	/* create notUnique index OSMWay.highway */
        OProperty oPropertyHighway = oSMWayClass.createProperty("highway", OType.STRING);
        oSMWayClass.createIndex("indexHighway", OClass.INDEX_TYPE.NOTUNIQUE, "highway");
    /* create unique index OSMNode.id */
        oSMNodeClass.createProperty("id", OType.LONG);
        oSMNodeClass.createProperty("amenity", OType.STRING);
        oSMNodeClass.createIndex("indexOSMNodeId", OClass.INDEX_TYPE.UNIQUE, "id");
        oSMNodeClass.createIndex("indexOSMNodeAmenity", OClass.INDEX_TYPE.NOTUNIQUE, "amenity");
	/* create unique index OSMWay.id */
        oSMWayClass.createProperty("id", OType.LONG);
        oSMWayClass.createIndex("indexOSMWayId", OClass.INDEX_TYPE.UNIQUE, "id");
	/* create unique index OSMRelation.id */
        oSMRelationClass.createProperty("id", OType.LONG);
        oSMRelationClass.createIndex("indexOSMRelationId", OClass.INDEX_TYPE.UNIQUE, "id");

        OGlobalConfiguration.ENVIRONMENT_CONCURRENT.setValue(false);

        long startTime = System.currentTimeMillis();
        database.declareIntent(new OIntentMassiveInsert());
        database.getLevel2Cache().setEnable(false);
        database.getLevel1Cache().setEnable(false);
        database.setLockMode(OGraphDatabase.LOCK_MODE.DATABASE_LEVEL_LOCKING);
        final OSMXMLReader reader = new OSMXMLReader();
        GeometryFactory geometryFactory = new GeometryFactory();
        try {
            int nbRaw = 0;
            log.trace(new File(pathnameOsmFile).getAbsolutePath());
            reader.setInput(new File(pathnameOsmFile));
            WKBWriter wkbWriter = new WKBWriter();

            while (reader.hasNext()) {
                final Object obj = reader.next();

                if (obj instanceof Node) {
                    Node node = (Node) obj;
                    ODocument nodeVertex = database.createVertex("OSM_NODE");
                    byte[] wkb = wkbWriter.write(geometryFactory.createPoint(new Coordinate(node.getLatitude(), node.getLongitude())));
                    if (wkb == null)
                        throw new NullPointerException("wkt is null");
                    nodeVertex.field("geom", wkb);
                    nodeVertex.field("id", node.getId());
                    writeTag(node, nodeVertex);
                    nodeVertex.save();
                    // log.trace("inserted node : " + node.getId());

                } else if (obj instanceof Way) {
                    Way way = (Way) obj;
                    ODocument wayVertex = database.createVertex("OSM_WAY");

                    wayVertex.field("id", way.getId());
                    writeTag(way, wayVertex);
                    wayVertex.save();
                    // log.trace("inserted way : " + way.getId());
                    int i = 0;
                    for (Long nodeId : way.getNodesIds()) {
                        ORID orid = getORidFromOSMId(database, nodeId, "NODE");

                        if (orid != null) {
                            ODocument edge = database.createEdge(wayVertex.getIdentity(), orid);
                            edge.field("ordinal", i);
                            i++;
                            edge.save();
                        }
                    }

                    wayVertex.save();

                } else if (obj instanceof Relation) {
                    Relation relation = (Relation) obj;
                    ODocument relationVertex = database.createVertex("OSM_RELATION");

                    relationVertex.field("id", relation.getId());
                    writeTag(relation, relationVertex);
                    relationVertex.save();
                    // log.trace("inserted relation : " + relation.getId());
                    for (Member member : relation.getMembers()) {
                        ORID orid = getORidFromOSMId(database, member.getReference(), member.getMemberType().toString());
                        if (orid != null) {
                            ODocument edge = database.createEdge(relationVertex.getIdentity(), orid);
                            edge.field("role", member.getRole());
                            edge.field("type", member.getMemberType());
                            edge.save();
                        }
                    }

                }

                if ((nbRaw++ % 100000) == 0) {
                    Period period = new Period(startTime, System.currentTimeMillis());
                    log.info(PeriodFormat.getDefault().print(period));
                    log.info("nb raw=" + nbRaw);
                    log.info("nb vertex=" + database.countVertexes());
                    log.info("nb edges=" + database.countEdges());

                }

            }

	    /* create spatial index OSMNode.geom */
            //createSpatialIndex(oSMNodeClass);

        } catch (Exception e) {
            Period period = new Period(startTime, System.currentTimeMillis());
            log.error("exception after " + PeriodFormat.getDefault().print(period), e);
            e.printStackTrace();
        } finally {
            reader.dispose();

        }
        Period period = new Period(startTime, System.currentTimeMillis());
        log.info("FINISH in " + PeriodFormat.getDefault().print(period) + " !!! YEAHHH");
        log.info("nb vertex=" + database.countVertexes());
        log.info("nb edges=" + database.countEdges());
        database.close();

    }

    private static ORID getORidFromOSMId(OGraphDatabase database,  Long osmId, String type) {
        OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(new StringBuilder().append("select from OSM_").append(type).append(" where id = ? ").toString());
        List<ODocument> result = database.command(query).execute(osmId);
        if (result.size() > 0) {
            return result.get(0).getIdentity();
        } else {
            log.warn("not found " + type + " " + osmId);
            return null;
        }
    }

    public static void createSpatialIndex(OClass OSMNodeClass) {
        long startTime = System.currentTimeMillis();
        if (OSMNodeClass.getProperty("geom") == null) {
            OProperty oProperty = OSMNodeClass.createProperty("geom", OType.BINARY);
            oProperty.setType(OType.BINARY);
        } else {
            OProperty oProperty = OSMNodeClass.getProperty("geom");
            oProperty.dropIndexes();
        }

        Period period = new Period(startTime, System.currentTimeMillis());
        log.info("creating spatial index in : " + PeriodFormat.getDefault().print(period));
    }

    public static OGraphDatabase resetDatabase(String OrientDbURL) {

        OGraphDatabase database = new OGraphDatabase(OrientDbURL);

        if (database.exists()) {
            database.open("admin", "admin").drop();
        }
        database.create();
        // database.open("admin", "admin");
        return database;

    }

    private static void writeTag(IdentifiedElement id, ODocument vertex) {
        for (Tag t : id.getTags()) {
            vertex.field(t.getK().replaceAll(":", "_").replaceAll(" ", "_").replaceAll(",", "_"), t.getV());
        }
    }

}
