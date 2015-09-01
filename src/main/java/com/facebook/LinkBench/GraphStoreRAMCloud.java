/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.facebook.LinkBench;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;
import edu.stanford.ramcloud.transactions.*;
import java.nio.ByteBuffer;
import org.apache.log4j.Level;

import org.apache.log4j.Logger;

/**
 *
 * @author ellitron
 */
public class GraphStoreRAMCloud extends GraphStore {

    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    
    /**
     * RAMCloud-specific configuration keys.
     */
    public static final String CONFIG_COORD_LOC = "coordinator_locator";
    public static final String CONFIG_ID_TABLE = "idtable";
    public static final String CONFIG_OUTLINKS_TABLE = "outlinkstable";
    
    /** 
     * Various settings for GraphStoreRAMCloud.
     */
    private static final int BYTEBUFFER_CAPACITY = 1024 * 1024 * 2;
    private static final int TX_MAX_RETRIES = 3;
    
    /**
     * The running RAMCloud cluster against which all operations and
     * transactions will be executed.
     */
    private RAMCloud ramcloud;
    
    /**
     * This table stores information for allocating element IDs. It has the 
     * following format:
     * 
     * key:             value: 
     * "largestNodeId"  long number recording largest ID of a node in the graph
     * 
     * Notes: 
     *  - "largestNodeID" is used to allocate unique node IDs by
     * incrementing this number each time a node is created in the graph.
     * 
     * Used to support the following API methods: 
     * - addNode
     */
    private long idTableId;
    
    private static final String largestNodeIdKey = "largestNodeId";
    
    /**
     * This table stores node meta-data. It has the following format:
     *
     * key:             value: 
     * (id, node type)  (version, time, data)
     * 
     * Used to support the following API methods: 
     * - addNode 
     * - getNode 
     * - updateNode 
     * - deleteNode
     */
    private long nodeTableId;
    
    /**
     * This table stores chronologically** sorted lists of outbound links,
     * including their meta-data, indexed by their source node id (denoted id1)
     * and link type. It has the following format:
     *
     * key:                     value: 
     * (id1, link type)         (N, (id2, visibility, version, time, data), ...)
     * (id1, link type, N)      ((id2, vis, ver, time, data), ...)
     * (id1, link type, N-1)    ((id2, vis, ver, time, data), ...)
     * (id1, link type, N-2)    ((id2, vis, ver, time, data), ...)
     * ...
     * (id1, link type, 1)      ((id2, vis, ver, time, data), ...)
     * 
     * Notes: 
     *  - "N" is the number of additional (key, value) pairs that are
     * being used to store this list of outbound links. This is used in cases
     * where the number of outbound links for a node exceeds the size of a
     * single RAMCloud (key, value) pair (currently limited to 1MB). It is also
     * used to increase performance by synthetically limiting the size of these
     * lists in order to reduce read times for getLinkList in the common case
     * (getting the most recent "rangeLimit" number of outbound links). At list
     * creation time, N=0. Before a new link is prepended to the list, the
     * newListSize is checked against listSizeThreshold. If exceeded, then
     * extensionSize (rounded up to fully capture any straddled links) is cut
     * from the tail end of the list (the oldest links in this list segment),
     * and stored in a new (key, value) pair with the key (id1, link type, N+1).
     * Therefore (id1, link type, 1) always stores the oldest links, followed by
     * (id1, link type, 2), and so on. Reading the head of the list at (id1,
     * link type) gives the client "N", all of which can then be read in
     * parallel.
     *  - Updating and deleting links given (id1, link type, id2) require a 
     * linear scan of the list. When found, the link is removed from
     * that location and the new link is prepended to the front of the list in
     * case of an update (we assume that all link updates always set the time
     * field to be the latest time).
     *  - ** Since adding and updating links always
     * prepend to the front of the link list, the list is sorted by timestamp
     * insofar as the requests arrive in timestamp order at the server, which is
     * in general not true. However, the time differential between out of order
     * links is (roughly) bounded by clock skews between clients and differences
     * in network delays between two clients to the RAMCloud server holding the
     * list. We assume that this the application is operating in a data-center
     * environment with network latencies and clock delays much much smaller
     * than the inter-arrival time of outbound link operations by users for a
     * given node in the graph, even for the hottest nodes.
     * 
     * Used to support the following API methods: 
     * - addLink 
     * - deleteLink 
     * - updateLink 
     * - getLink 
     * - getLinkList
     */
    private long outLinksTableId;

    /**
     * The service locator for the RAMCloud coordinator.
     */
    private String coordinatorLocator;

    /**
     * Name of the ID table in RAMCloud.
     */
    private String idTable;
    
    /**
     * Name of the node table in RAMCloud.
     */
    private String nodeTable;

    /**
     * Name of the outbound link table in RAMCloud.
     */
    private String outLinksTable;

    /**
     * Used for serializing and de-serializing keys and values.
     */
    private ByteBuffer keyByteBuffer;
    private ByteBuffer valueByteBuffer;
    
    public GraphStoreRAMCloud() {
        super();
        logger.setLevel(Level.DEBUG);
    }

    /**
     * Initializes this GraphStoreRAMCloud object, and ensures that the target
     * RAMCloud cluster is also properly initialized.
     *
     * For initializing this object instance, this primarily includes setting
     * its configuration parameters using the LinkBench and RAMCloud
     * configuration settings in the "LinkConfigRAMCloud.properties"
     * configuration file, and establishing a connection to the RAMCloud cluster
     * specified by the "coordinator_locator" parameter in the configuration
     * file (note that, therefore, a RAMCloud cluster should already be running
     * with its coordinator at that location before invocation of this method).
     *
     * Regarding the target RAMCloud cluster, this method ensures that all the
     * needed tables exist and have been initialized before the method returns.
     * If they are already existing and initialized then no action is taken.
     * This is done in such a way that multiple GraphStoreRAMCloud objects using
     * the same cluster may invoke this method at the same time safely.
     *
     * @param p
     * @param currentPhase
     * @param threadId
     * @throws IOException
     * @throws Exception
     */
    @Override
    public void initialize(Properties p, Phase currentPhase, int threadId)
            throws IOException, Exception {
        coordinatorLocator = ConfigUtil.getPropertyRequired(p, CONFIG_COORD_LOC);
        idTable = ConfigUtil.getPropertyRequired(p, CONFIG_ID_TABLE);
        nodeTable = ConfigUtil.getPropertyRequired(p, Config.NODE_TABLE);
        outLinksTable = ConfigUtil.getPropertyRequired(p, CONFIG_OUTLINKS_TABLE);
        
        keyByteBuffer = ByteBuffer.allocate(BYTEBUFFER_CAPACITY);
        valueByteBuffer = ByteBuffer.allocate(BYTEBUFFER_CAPACITY);
        
        // Attempt to connect to the target RAMCloud cluster
        try {
            ramcloud = new RAMCloud(coordinatorLocator);
        } catch(ClientException e) {
            // TODO: Handle exception
        }
        
        // When tables already exist, createTable simply returns the table ID
        idTableId = ramcloud.createTable(idTable);
        nodeTableId = ramcloud.createTable(nodeTable);
        outLinksTableId = ramcloud.createTable(outLinksTable);
    }

    /**
     * Performs any cleanup before this GraphStoreRAMCloud object is
     * de-constructed. Does not modify any data in RAMCloud.
     */
    @Override
    public void close() {
        ramcloud.disconnect();
    }

    @Override
    public void clearErrors(int threadID) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        logger.debug("resetNodeStore: startID=" + startID);
        
        ramcloud.dropTable(nodeTable);
        nodeTableId = ramcloud.createTable(nodeTable);
        ByteBuffer bb = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
        bb.putLong(startID - 1);
        ramcloud.write(idTableId, largestNodeIdKey, bb.array());
    }
    
    /** 
     * These helper methods encapsulate the logic for serializing LinkBench
     * Nodes to/from RAMCloudObjects.
     */
    private Node parseNode(RAMCloudObject obj) {
        long id;
        int type;
        long version;
        int time;
        byte data[];
        
        ByteBuffer key = ByteBuffer.allocate((Long.SIZE + Integer.SIZE)/Byte.SIZE);
        key.put(obj.getKeyBytes()).flip();
        id = key.getLong();
        type = key.getInt();
        
        ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
        value.put(obj.getValueBytes()).flip();
        version = value.getLong();
        time = value.getInt();
        data = new byte[value.remaining()];
        value.get(data);
        
        return new Node(id, type, version, time, data);
    }
    
    private RAMCloudObject serializeNode(Node node) {
        ByteBuffer key = ByteBuffer.allocate((Long.SIZE + Integer.SIZE)/Byte.SIZE);
        key.putLong(node.id);
        key.putInt(node.type);
        
        int size;
        if(node.data != null) {
            size = (Long.SIZE + Integer.SIZE) / Byte.SIZE + node.data.length;
        } else {
            size = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
        }
        
        ByteBuffer value = ByteBuffer.allocate(size);
        value.putLong(node.version);
        value.putInt(node.time);
        
        if(node.data != null)
            value.put(node.data);
        
        return new RAMCloudObject(key.array(), value.array(), 0);
    }
    
    /**
     * See
     * {@link com.facebook.LinkBench.NodeStore#addNode(java.lang.String, com.facebook.LinkBench.Node) addNode}.
     */
    @Override
    public long addNode(String dbid, Node node) throws Exception {
        Node newNode = node.clone();
        int retries = 0;
        
        while(true) {
            Transaction tx = new Transaction(ramcloud);
            
            /* Increment largest node ID and use for new ID */
            RAMCloudObject obj = tx.read(idTableId, largestNodeIdKey);
            ByteBuffer buf = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
            buf.put(obj.getValueBytes()).flip();
            long largestNodeId = buf.getLong();
            largestNodeId++;
            buf.clear();
            buf.putLong(largestNodeId).flip();
            tx.write(idTableId, largestNodeIdKey, buf.array());
            
            /* Write new node into node table */
            newNode.id = largestNodeId;
            
            obj = serializeNode(newNode);
            tx.write(nodeTableId, obj.getKeyBytes(), obj.getValueBytes());
            if(tx.commitAndSync()) {
                logger.debug("addNode: added node=" + newNode.toString());
                return largestNodeId;
            } else {
                retries++;
                if(retries > TX_MAX_RETRIES)
                    throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
            }
        }
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        logger.debug("getNode: node.id=" + id + " node.type=" + type);
                
        Node node = new Node(id, type, 0, 0, null);
        RAMCloudObject obj = serializeNode(node);
        try {
            obj = ramcloud.read(nodeTableId, obj.getKeyBytes());
        } catch(ObjectDoesntExistException e) {
            return null;
        } catch(ClientException e) {
            logger.error("getNode (" + node.toString() + ") encountered exception: " + e.toString());
            throw e;
        }
        
        node = parseNode(obj);
        
        return node;
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        logger.debug("updateNode: node=" + node.toString());
        
        RAMCloudObject obj = serializeNode(node);
        RejectRules rules = new RejectRules();
        rules.rejectIfDoesntExist(true);
        try { 
            ramcloud.write(nodeTableId, obj.getKeyBytes(), obj.getValueBytes(), rules);
        } catch(ObjectDoesntExistException e) {
            return false;
        }
        
        return true;
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        logger.debug("deleteNode: node.id=" + id + " node.type=" + type);
        
        Node node = new Node(id, type, 0, 0, null);
        RAMCloudObject obj = serializeNode(node);
        RejectRules rules = new RejectRules();
        rules.rejectIfDoesntExist(true);
        try {
            ramcloud.remove(nodeTableId, obj.getKeyBytes(), rules);
        } catch(ObjectDoesntExistException e) {
            return false;
        } 
        
        return true;
    }

    @Override
    public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
