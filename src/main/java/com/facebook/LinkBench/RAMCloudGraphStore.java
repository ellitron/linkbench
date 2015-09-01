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
import edu.stanford.ramcloud.multiop.MultiReadObject;
import edu.stanford.ramcloud.transactions.*;
import java.nio.ByteBuffer;
import org.apache.log4j.Level;

import org.apache.log4j.Logger;

/**
 *
 * @author ellitron
 */
public class RAMCloudGraphStore extends GraphStore {

    private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);
    
    /**
     * RAMCloud-specific configuration keys.
     */
    public static final String CONFIG_MASTER_SERVERS = "master_servers";
    public static final String CONFIG_COORD_LOC = "coordinator_locator";
    public static final String CONFIG_ID_TABLE = "idtable";
    public static final String CONFIG_OUTLINKS_TABLE = "outlinkstable";
    public static final String CONFIG_LINKLIST_HEADSIZETHRESHOLD = "linklist_headsizethreshold";
    public static final String CONFIG_LINKLIST_STUMPSIZE = "linklist_stumpsize";
    
    public static final int DEFAULT_BULKINSERT_SIZE = 1024;
    
    /** 
     * Various settings for RAMCloudGraphStore.
     */
    private static final int BYTEBUFFER_CAPACITY = 1024 * 1024 * 2;
    private static final int TX_MAX_RETRIES = 1000;
    private static final int TX_FAIL_REPORT_COUNT = 100;
    
    /**
     * The running RAMCloud cluster against which all operations and
     * transactions will be executed.
     */
    private RAMCloud ramcloud;
    
    /**
     * The Transaction object which is re-used for each transaction performed. 
     * Before using this to perform a transaction, the clear() method should 
     * always be called to clear any previous reads and writes.
     */
    private Transaction tx;
    
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
     * key:                 value: 
     * (id, node type)      (version, time, data)
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
     * (id1, link type)         (N, (id2, visibility, version, time, datalen, data), ...)
     * (id1, link type, N-1)    ((id2, vis, ver, time, datalen, data), ...)
     * (id1, link type, N-2)    ((id2, vis, ver, time, datalen, data), ...)
     * ...
     * (id1, link type, 1)      ((id2, vis, ver, time, datalen, data), ...)
     * (id1, link type, 0)      ((id2, vis, ver, time, datalen, data), ...)
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
     * and stored in a new (key, value) pair with the key (id1, link type, N),
     * and N is incremented for the head segment. Therefore (id1, link type, 0)
     * always stores the oldest links, followed by (id1, link type, 1), and so
     * on. Reading the head of the list at (id1, link type) gives the client
     * "N", all of which can then be read in parallel.
     *  - "datalen" is the length, in bytes, of the data part.
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
    
    /* Size of (id2, vis, ver, time, datalen) */
    private static int LINK_ID2_OFFSET = 0;
    private static int LINK_VIS_OFFSET = (Long.SIZE) / Byte.SIZE;
    private static int LINK_VER_OFFSET = (Long.SIZE + Byte.SIZE) / Byte.SIZE;
    private static int LINK_TIME_OFFSET = (Long.SIZE + Byte.SIZE + Integer.SIZE) / Byte.SIZE;
    private static int LINK_DLEN_OFFSET = (Long.SIZE + Byte.SIZE + Integer.SIZE + Long.SIZE) / Byte.SIZE;
    private static int LINK_HEADER_SIZE = (Long.SIZE + Byte.SIZE + Integer.SIZE + Long.SIZE + Integer.SIZE) / Byte.SIZE;
    
    private Level debugLevel;
    
    /**
     * The number of RAMCloud master servers to use for storing tables.
     */
    private int totalMasterServers;
    
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
     * Max size, in bytes, of the head segment of a link list.
     */
    private int linkListHeadSizeThreshold;
    
    /**
     * Target result size, in bytes, of the head segment after extruding off a 
     * new tail segment.
     */
    private int linkListStumpSize;
    
    /**
     * Stats for recording
     */
    private long outLinksTableValueSize;
    private long nodeTableValueSize;
    
    private Phase phase;
    
    /**
     * Used for serializing and de-serializing keys and values.
     */
    private ByteBuffer keyByteBuffer;
    private ByteBuffer valueByteBuffer;
    
    public RAMCloudGraphStore() {
        super();
        logger.setLevel(Level.INFO);
    }

    /**
     * Initializes this RAMCloudGraphStore object, and ensures that the target
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
     * This is done in such a way that multiple RAMCloudGraphStore objects using
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
        debugLevel = ConfigUtil.getDebugLevel(p);
        logger.setLevel(debugLevel);
        logger.debug("initialize()");
        totalMasterServers = ConfigUtil.getInt(p, CONFIG_MASTER_SERVERS);
        coordinatorLocator = ConfigUtil.getPropertyRequired(p, CONFIG_COORD_LOC);
        idTable = ConfigUtil.getPropertyRequired(p, CONFIG_ID_TABLE);
        nodeTable = ConfigUtil.getPropertyRequired(p, Config.NODE_TABLE);
        outLinksTable = ConfigUtil.getPropertyRequired(p, CONFIG_OUTLINKS_TABLE);
        linkListHeadSizeThreshold = ConfigUtil.getInt(p, CONFIG_LINKLIST_HEADSIZETHRESHOLD);
        linkListStumpSize = ConfigUtil.getInt(p, CONFIG_LINKLIST_STUMPSIZE);
        
        keyByteBuffer = ByteBuffer.allocate(BYTEBUFFER_CAPACITY);
        valueByteBuffer = ByteBuffer.allocate(BYTEBUFFER_CAPACITY);
        
        // Attempt to connect to the target RAMCloud cluster
        try {
            ramcloud = new RAMCloud(coordinatorLocator);
        } catch(ClientException e) {
            logger.error("initialize(): Could not establish connection with coordinator @ " + coordinatorLocator);
            throw e;
        }
        
        tx = new Transaction(ramcloud);
        
        // When tables already exist, createTable simply returns the table ID
        idTableId = ramcloud.createTable(idTable);
        nodeTableId = ramcloud.createTable(nodeTable, totalMasterServers);
        outLinksTableId = ramcloud.createTable(outLinksTable, totalMasterServers);
        
        nodeTableValueSize = 0;
        outLinksTableValueSize = 0;
        
        phase = currentPhase;
    }

    /**
     * @return 0 if it doesn't support addBulkLinks and recalculateCounts
     * methods If it does support them, return the maximum number of links that
     * can be added at a time
     */
    public int bulkLoadBatchSize() {
        return DEFAULT_BULKINSERT_SIZE;
    }
  
    /**
     * Performs any cleanup before this RAMCloudGraphStore object is
     * de-constructed. Does not modify any data in RAMCloud.
     */
    @Override
    public void close() {
        if(phase == Phase.LOAD) {
            logger.info("Total Size of Nodes Loaded: " + nodeTableValueSize + " Bytes");
            logger.info("Total Size of Links Loaded: " + outLinksTableValueSize + " Bytes");
        }
        
        ramcloud.disconnect();
    }

    public void dropTables() {
        ramcloud.dropTable(idTable);
        ramcloud.dropTable(nodeTable);
        ramcloud.dropTable(outLinksTable);
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
    
    private RAMCloudObject linkListMakeNewHead(Link initLink, boolean fillHead) {
        byte[] headKey = linkListMakeHeadKey(initLink);
        
        ByteBuffer buf;
        if (fillHead) {
            buf = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE + LINK_HEADER_SIZE + initLink.data.length);
            buf.putInt(0);
            buf.putLong(initLink.id2);
            buf.put(initLink.visibility);
            buf.putInt(initLink.version);
            buf.putLong(initLink.time);
            buf.putInt(initLink.data.length);
            buf.put(initLink.data);
        } else {
            buf = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
            buf.putInt(0);
        }
        
        return new RAMCloudObject(headKey, buf.array(), 0);
    }
    
    private byte[] linkListMakeHeadKey(Link link) {
        int keySize = (Long.SIZE + Long.SIZE) / Byte.SIZE;
        ByteBuffer buf = ByteBuffer.allocate(keySize);

        buf.putLong(link.id1);
        buf.putLong(link.link_type);

        return buf.array();
    }

    private byte[][] linkListMakeTailKeys(RAMCloudObject headObj) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        buf.put(headObj.getValueBytes(), 0, Integer.SIZE / Byte.SIZE);
        buf.flip();
        int N = buf.getInt();

        if (N != 0) {
            byte[][] tailKeys = new byte[N][];
            for (int i = 0; i < N; i++) {
                ByteBuffer tailKey = ByteBuffer.allocate((Long.SIZE + Long.SIZE + Integer.SIZE) / Byte.SIZE);
                tailKey.put(headObj.getKeyBytes());
                tailKey.putInt(i);
                tailKeys[i] = tailKey.array();
            }

            return tailKeys;
        }

        return null;
    }
    
    /**
     * Link list operation codes
     */
    public enum LinkListOpCode {
        EXPUNGE,
        HIDE,
        GET;
    }
    
    private int linkListFindLink(RAMCloudObject headObj, RAMCloudObject[] tailObjs, Link link, LinkListOpCode op) throws Exception {
        int totalSegments = 1;

        if (tailObjs != null) {
            totalSegments += tailObjs.length;
        }

        ByteBuffer buf;
        for (int i = totalSegments - 1; i >= 0; i--) {
            if (i == totalSegments - 1) {
                buf = ByteBuffer.allocate(headObj.getValueBytes().length);
                buf.put(headObj.getValueBytes());
            } else {
                buf = ByteBuffer.allocate(tailObjs[i].getValueBytes().length);
                buf.put(tailObjs[i].getValueBytes());
            }

            buf.flip();

            if (i == totalSegments - 1) {
                int tailSegments = buf.getInt();
                if (tailSegments+1 != totalSegments) {
                    logger.error("linkListFindLink: head segment records " + tailSegments + " tail segments, but we calculated " + (totalSegments-1) + " tail segments");
                    throw new Exception();
                }
            }

            while (buf.hasRemaining()) {
                int linkStartPos = buf.position();
                long id2 = buf.getLong();

                if (link.id2 == id2) {
                    link.visibility = buf.get();
                    link.version = buf.getInt();
                    link.time = buf.getLong();
                    int datalen = buf.getInt();
                    byte[] data = new byte[datalen];
                    buf.get(data);
                    link.data = data;

                    if (op == LinkListOpCode.EXPUNGE) {
                        byte[] newValue = new byte[buf.capacity() - LINK_HEADER_SIZE - datalen];
                        int nextLinkStartPos = buf.position();
                        buf.rewind();
                        buf.get(newValue, 0, linkStartPos);
                        buf.position(nextLinkStartPos);
                        buf.get(newValue, linkStartPos, buf.remaining());

                        if (i == totalSegments - 1) {
                            headObj.setValueBytes(newValue);
                        } else {
                            tailObjs[i].setValueBytes(newValue);
                        }
                    } else if(op == LinkListOpCode.HIDE) {
                        buf.position(linkStartPos + Long.SIZE/Byte.SIZE);
                        buf.put(VISIBILITY_HIDDEN);
                        if (i == totalSegments - 1) {
                            headObj.setValueBytes(buf.array());
                        } else {
                            tailObjs[i].setValueBytes(buf.array());
                        }
                    } else if(op == LinkListOpCode.GET) {
                        
                    } else {
                        logger.error("linkListFindLink: Unrecognized op code");
                        throw new Exception();
                    }

                    return i;
                } else {
                    int datalen = buf.getInt(linkStartPos + LINK_HEADER_SIZE - (Integer.SIZE / Byte.SIZE));
                    buf.position(linkStartPos + LINK_HEADER_SIZE + datalen);
                }
            }
        }

        return -1; // Not found
    }

    private RAMCloudObject linkListPrependToHead(Link link, RAMCloudObject headObj) throws Exception {
        int linkSize = LINK_HEADER_SIZE + link.data.length;
        int headSize = headObj.getValueBytes().length;
        ByteBuffer buf = ByteBuffer.allocate(linkSize + headSize);
        
        buf.put(headObj.getValueBytes(), 0, Integer.SIZE/Byte.SIZE);
        
        buf.putLong(link.id2);
        buf.put(link.visibility);
        buf.putInt(link.version);
        buf.putLong(link.time);
        buf.putInt(link.data.length);
        buf.put(link.data);
        
        buf.put(headObj.getValueBytes(), Integer.SIZE/Byte.SIZE, headSize - Integer.SIZE/Byte.SIZE);
        
        headObj.setValueBytes(buf.array());
        
        return linkListEnforceHeadLimits(headObj);
    }
    
    private int linkListInsert(RAMCloudObject headObj, RAMCloudObject[] tailObjs, Link link) throws Exception {
        int totalSegments = 1;

        if (tailObjs != null)
            totalSegments += tailObjs.length;

        ByteBuffer buf;
        for (int i = totalSegments - 1; i >= 0; i--) {
            if (i == totalSegments - 1) {
                buf = ByteBuffer.allocate(headObj.getValueBytes().length);
                buf.put(headObj.getValueBytes());
            } else {
                buf = ByteBuffer.allocate(tailObjs[i].getValueBytes().length);
                buf.put(tailObjs[i].getValueBytes());
            }

            buf.flip();

            if (i == totalSegments - 1) {
                int tailSegments = buf.getInt();
                if (tailSegments+1 != totalSegments) {
                    logger.error("linkListInsert: head segment records " + tailSegments + " tail segments, but we calculated " + (totalSegments-1) + " tail segments");
                    throw new Exception();
                }
            }

            while (buf.hasRemaining()) {
                int linkStartPos = buf.position();
                long time = buf.getLong(linkStartPos + LINK_TIME_OFFSET);
                
                if(link.time >= time) {
                    ByteBuffer serializedLink = ByteBuffer.allocate(LINK_HEADER_SIZE + link.data.length);
                    serializedLink.putLong(link.id2);
                    serializedLink.put(link.visibility);
                    serializedLink.putInt(link.version);
                    serializedLink.putLong(link.time);
                    serializedLink.putInt(link.data.length);
                    serializedLink.put(link.data);
                    serializedLink.flip();
                    
                    byte[] newValue = new byte[buf.capacity() + serializedLink.capacity()];
                    buf.rewind();
                    buf.get(newValue, 0, linkStartPos);
                    serializedLink.get(newValue, linkStartPos, serializedLink.capacity());
                    buf.get(newValue, linkStartPos + serializedLink.capacity(), buf.remaining());
                    
                    if(i == totalSegments - 1)
                        headObj.setValueBytes(newValue);
                    else 
                        tailObjs[i].setValueBytes(newValue);
                    
                    return i;
                } else {
                    int datalen = buf.getInt(linkStartPos + LINK_DLEN_OFFSET);
                    buf.position(linkStartPos + LINK_HEADER_SIZE + datalen);
                }
            }
            
            if(i == 0) {
                ByteBuffer serializedLink = ByteBuffer.allocate(LINK_HEADER_SIZE + link.data.length);
                serializedLink.putLong(link.id2);
                serializedLink.put(link.visibility);
                serializedLink.putInt(link.version);
                serializedLink.putLong(link.time);
                serializedLink.putInt(link.data.length);
                serializedLink.put(link.data);
                serializedLink.flip();

                byte[] newValue = new byte[buf.capacity() + serializedLink.capacity()];
                buf.rewind();
                buf.get(newValue, 0, buf.capacity());
                serializedLink.get(newValue, buf.capacity(), serializedLink.capacity());

                if (totalSegments == 1) {
                    headObj.setValueBytes(newValue);
                } else {
                    tailObjs[0].setValueBytes(newValue);
                }

                return 0;
            }
        }
        
        return -1; // Something went wrong
    }
    
    private RAMCloudObject linkListEnforceHeadLimits(RAMCloudObject headObj) throws Exception {
        
        if (headObj.getValueBytes().length > linkListHeadSizeThreshold) {
            ByteBuffer buf = ByteBuffer.allocate(headObj.getValueBytes().length);
            buf.put(headObj.getValueBytes());
            buf.flip();
            
            int newTailSegments = buf.getInt() + 1;
            
            buf.rewind();
            buf.putInt(newTailSegments);
            
            if(buf.position() >= linkListStumpSize) {
                logger.error("linkListPrependToHead: linkListStumpSize=" + linkListStumpSize + " too small. First link starting position=" + buf.position());
                throw new Exception();
            }
            
            while (buf.hasRemaining()) {
                int linkStartPos = buf.position();
                int datalen = buf.getInt(linkStartPos + LINK_HEADER_SIZE - (Integer.SIZE / Byte.SIZE));
                int nextLinkStartPos = linkStartPos + LINK_HEADER_SIZE + datalen;
                
                if(buf.remaining() < nextLinkStartPos - linkStartPos) {
                    logger.error("linkListPrependToHead: Incomplete link at end of list. Link size is " + (LINK_HEADER_SIZE + datalen) + " but buf.remaining()=" + buf.remaining());
                    throw new Exception();
                }
                
                if(nextLinkStartPos >= linkListStumpSize) {
                    int leftDist = linkListStumpSize - linkStartPos;
                    int rightDist = nextLinkStartPos - linkListStumpSize;
                    int chopIndex;
                    
                    if(nextLinkStartPos >= linkListHeadSizeThreshold)
                        chopIndex = linkStartPos;
                    else if(rightDist < leftDist)
                        chopIndex = nextLinkStartPos;
                    else
                        chopIndex = linkStartPos;
                    
                    byte[] newHeadValue = new byte[chopIndex];
                    byte[] tailValue = new byte[buf.capacity() - chopIndex];
                    
                    buf.rewind();
                    buf.get(newHeadValue);
                    buf.get(tailValue);
                    
                    ByteBuffer tailKey = ByteBuffer.allocate((Long.SIZE + Long.SIZE + Integer.SIZE) / Byte.SIZE);
                    tailKey.put(headObj.getKeyBytes());
                    tailKey.putInt(newTailSegments - 1);
                    
                    headObj.setValueBytes(newHeadValue);
                    return new RAMCloudObject(tailKey.array(), tailValue, 0);
                }
                
                buf.position(nextLinkStartPos);
            }
            
            logger.error("linkListPrependToHead: This is embarassing... failed to find a chop point!");
            throw new Exception();
        } else
            return null;
    }
    
    private class SearchState {
        public int offset;
        public int limit;
        
        public SearchState(int offset, int limit) {
            this.offset = offset;
            this.limit = limit;
        }
    }
    
    private boolean parseLinkList(  ArrayList<Link> dst,
                                    RAMCloudObject listObj,
                                    boolean isHead,
                                    long maxTimestamp,
                                    long minTimestamp,
                                    SearchState state) throws Exception {
        // Sanity checks.
        if(state.limit == 0)
            return false;
        
        if(maxTimestamp < minTimestamp) {
            logger.error("parseLinkList: maxTimestamp=" + maxTimestamp + " minTimestamp=" + minTimestamp);
            throw new Exception();
        }
        
        ByteBuffer buf = ByteBuffer.allocate(listObj.getKeyBytes().length);
        buf.put(listObj.getKeyBytes());
        buf.flip();
        long id1 = buf.getLong();
        long type = buf.getLong();
        
        buf = ByteBuffer.allocate(listObj.getValueBytes().length);
        buf.put(listObj.getValueBytes());
        buf.flip();
        
        if(isHead)
            buf.getInt();
        
        while(buf.hasRemaining()) {
            int linkStartPos = buf.position();
            long time = buf.getLong(linkStartPos + LINK_TIME_OFFSET);
            
            if(time <= maxTimestamp && time >= minTimestamp) {
                byte visibility = buf.get(linkStartPos + LINK_VIS_OFFSET);
                int datalen = buf.getInt(linkStartPos + LINK_DLEN_OFFSET);
                
                if(visibility != LinkStore.VISIBILITY_HIDDEN) {
                    if (state.offset > 0) {
                        state.offset--;
                        buf.position(linkStartPos + LINK_HEADER_SIZE + datalen);
                    } else {
                        long id2 = buf.getLong(linkStartPos + LINK_ID2_OFFSET);
                        int version = buf.getInt(linkStartPos + LINK_VER_OFFSET);
                        byte[] data = new byte[datalen];
                        buf.position(linkStartPos + LINK_HEADER_SIZE);
                        buf.get(data);
                        dst.add(new Link(id1, type, id2, visibility, data, version, time));

                        state.limit--;
                        if (state.limit == 0) {
                            return false;
                        }
                    }
                } else 
                    buf.position(linkStartPos + LINK_HEADER_SIZE + datalen);
            } else if(time > maxTimestamp) {
                // Then keep moving down the list
                int datalen = buf.getInt(linkStartPos + LINK_DLEN_OFFSET);
                buf.position(linkStartPos + LINK_HEADER_SIZE + datalen);
            } else if(time < minTimestamp) {
                // Then we've exited the time window. Stop.
                return false;
            }
        }
        
        return true;
    }
    
    private int linkListCountLinks(RAMCloudObject headObj, RAMCloudObject[] tailObjs) throws Exception {
        int linkCount = 0;
        int totalBytes = 0;
        int totalSegments = 1;

        if (tailObjs != null) {
            totalSegments += tailObjs.length;
        }

        ByteBuffer buf;
        for (int i = totalSegments - 1; i >= 0; i--) {
            if (i == totalSegments - 1) {
                buf = ByteBuffer.allocate(headObj.getValueBytes().length);
                buf.put(headObj.getValueBytes());
            } else {
                buf = ByteBuffer.allocate(tailObjs[i].getValueBytes().length);
                buf.put(tailObjs[i].getValueBytes());
            }

            buf.flip();

            if (i == totalSegments - 1) {
                int tailSegments = buf.getInt();
                if (tailSegments+1 != totalSegments) {
                    logger.error("linkListFindLink: head segment records " + tailSegments + " tail segments, but we calculated " + (totalSegments-1) + " tail segments");
                    throw new Exception();
                }
            }

            int segLinkCount = 0;
            int totalDataLen = 0;
            while (buf.hasRemaining()) {
                int linkStartPos = buf.position();
                int datalen = buf.getInt(linkStartPos + LINK_DLEN_OFFSET);
                byte visibility = buf.get(linkStartPos + LINK_VIS_OFFSET);
                
                if(buf.remaining() < LINK_HEADER_SIZE + datalen) {
                    logger.error("linkListCountLinks: Incomplete link in buffer");
                    throw new Exception();
                }
                
                if(visibility != LinkStore.VISIBILITY_HIDDEN)
                    segLinkCount++;
                
                buf.position(linkStartPos + LINK_HEADER_SIZE + datalen);
                
                totalDataLen += datalen;
            }
            
            linkCount += segLinkCount;
            totalBytes += buf.capacity();
            
            //logger.trace("linkListCountLinks: seg " + i + ": size=" + buf.capacity() + ", links=" + segLinkCount + ", avgDataLen=" + (double)totalDataLen/(double)segLinkCount);
        }
        
        //logger.trace("linkListCountLinks: totalBytes=" + totalBytes + ", bytesPerSeg=" + totalBytes/totalSegments + ", bytesPerLink=" + totalBytes/linkCount);

        return linkCount; 
    }
    
    /**
     * See
     * {@link com.facebook.LinkBench.NodeStore#addNode(java.lang.String, com.facebook.LinkBench.Node) addNode}.
     */
    @Override
    public long addNode(String dbid, Node node) throws Exception {
        Node newNode = node.clone();
        int retries = 0;
        
        while(retries < TX_MAX_RETRIES) {
            tx.clear();
            
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
                logger.debug("addNode: added node=(id=" + newNode.id + ")");
                nodeTableValueSize += obj.getValueBytes().length;
                
                return largestNodeId;
            }
            
            retries++;
            
            if(retries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("addNode: transaction failed " + retries + " times");
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
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

    /**
     * This method adds a new link to the graph, or updates an existing one 
     * if it already exists. 
     * 
     * This method first reads the head of the link list at (id1, type). If this
     * object does not exist, then this link is the first in the list, and a new
     * list is created with this link at the head. If this object does exist,
     * then we need to determine if the link already exists in the list. First,
     * we scan this list head for the item. If it is found, then we splice out
     * this old link, and append the new link to the front, and write the list
     * back. If it is not found, we see if there is a tail and how many segments
     * are in the tail by checking if N > 0. If N==0, then there are no tail
     * segments and this is a new link. If N > 0, then we read those N segments
     * and continue our search. If the link is found in segment i, then it is
     * spliced out of segment i, appended to the head, and the head and segment
     * i are written back. If it was not found, then it is simply written to the
     * head segment.
     *
     * In all cases, prepending the new/updated link to the front of the head
     * segment may increase its size, and bounds checking must be performed to
     * see if we need to chop off a new extension segment. If the total number
     * of bytes exceeds headSizeThreshold, then we need to chop. In this case,
     * we leave stumpSize bytes of the head remaining and move the rest to
     * extension N+1. But since we must chop on link-aligned boundaries and in
     * general stumpSize will land inside a link, we must pick a nearby
     * chop-point. We adopt the simple policy of chopping at the nearest
     * boundary, either above or below, unless the nearest point exceeds
     * headSizeThreshold, in which case we choose the lower boundary, no matter
     * how far away it is.
     * 
     * @param dbid
     * @param a
     * @param noinverse
     * @return
     * @throws Exception 
     */
    @Override
    public boolean addLink(String dbid, Link newLink, boolean noinverse) throws Exception {
        if(noinverse == false)
            throw new UnsupportedOperationException("Inverse not supported yet.");
        
        logger.debug("addLink: adding newLink=" + newLink.toString());
        
        byte[] headKey = linkListMakeHeadKey(newLink);
        
        int txRetries = 0;
        while(txRetries < TX_MAX_RETRIES) {
            tx.clear();
            
            /** Transaction code goes here. */
            
            RAMCloudObject headObj;
            try {
                headObj = tx.read(outLinksTableId, headKey);
            } catch (ObjectDoesntExistException e) {
                RAMCloudObject newHeadObj = linkListMakeNewHead(newLink, true);
                tx.write(outLinksTableId, newHeadObj.getKeyBytes(), newHeadObj.getValueBytes());
                
                if(tx.commitAndSync())
                    return true;
                
                txRetries++;
                
                if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                    logger.warn("addLink: transaction failed " + txRetries + " times");
                
                continue;
            }

            byte[][] tailKeys = linkListMakeTailKeys(headObj);
            RAMCloudObject[] tailObjs = null;
            int tailSegments = 0;
            if (tailKeys != null) {
                tailSegments = tailKeys.length;
                tailObjs = new RAMCloudObject[tailSegments];
                for (int i = 0; i < tailSegments; i++) {
                    tailObjs[i] = tx.read(outLinksTableId, tailKeys[i]);
                }
            }

            Link oldLink = newLink.clone();
            int oldLinkSegIndex = linkListFindLink(headObj, tailObjs, oldLink, LinkListOpCode.EXPUNGE);
            int newLinkSegIndex = linkListInsert(headObj, tailObjs, newLink);

            if (newLinkSegIndex == -1) {
                logger.error("addLink(): linkListInsert returned -1");
                throw new Exception();
            }

            if (newLinkSegIndex == tailSegments) {
                RAMCloudObject newTailSeg = linkListEnforceHeadLimits(headObj);
                if (newTailSeg != null) {
                    tx.write(outLinksTableId, newTailSeg.getKeyBytes(), newTailSeg.getValueBytes());
                }
            }

            if (newLinkSegIndex == oldLinkSegIndex) {
                if (newLinkSegIndex == tailSegments) {
                    tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());
                } else {
                    tx.write(outLinksTableId, tailObjs[newLinkSegIndex].getKeyBytes(), tailObjs[newLinkSegIndex].getValueBytes());
                }
            } else {
                if (newLinkSegIndex == tailSegments) {
                    tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());
                } else {
                    tx.write(outLinksTableId, tailObjs[newLinkSegIndex].getKeyBytes(), tailObjs[newLinkSegIndex].getValueBytes());
                }

                if (oldLinkSegIndex != -1) {
                    if (oldLinkSegIndex == tailSegments) {
                        tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());
                    } else {
                        tx.write(outLinksTableId, tailObjs[oldLinkSegIndex].getKeyBytes(), tailObjs[oldLinkSegIndex].getValueBytes());
                    }
                }
            }
            
            if(tx.commitAndSync()) {
                if (oldLinkSegIndex != -1) {
                    return false; // Updated
                } else
                    return true; // Totally new link added
            }
            
            txRetries++;
            
            if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("addLink: transaction failed " + txRetries + " times");
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
    }
    
    /**
     * Add a batch of links without updating counts
     * 
     * This method assumes that links in the list are 
     */
    public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
            throws Exception {
        if(noinverse == false)
            throw new UnsupportedOperationException("Inverse not supported yet.");
        
        if(links.isEmpty()) 
            return;
        
        logger.debug("addBulkLinks: adding " + links.size() + " links");
        
        int txRetries = 0;
        while(txRetries < TX_MAX_RETRIES) {
            tx.clear();
            
            int startId1Index = 0;
            for(int i = startId1Index + 1; i < links.size(); i++) {
                if(links.get(i).id1 != links.get(i-1).id1) {
                    // From startIndex to i-1 are all links
                    // belonging to the same id1
                    int startTypeIndex = startId1Index;
                    for(int j = startTypeIndex + 1; j < i; j++) {
                        if(links.get(j).link_type != links.get(j-1).link_type) {
                            logger.debug("addBulkLinks: " + (j-startTypeIndex) + "/" + links.size() + " are for (id1=" + links.get(startTypeIndex).id1 + ", type=" + links.get(startTypeIndex).link_type + ")");
                            // From startTypeIndex to j-1 are all links
                            // belonging to the same Id1 and with the same type
                            // We are going to take this range of links in this 
                            // list and bulk insert them 
                            byte[] headKey = linkListMakeHeadKey(links.get(startTypeIndex));

                            RAMCloudObject headObj;
                            try {
                                headObj = tx.read(outLinksTableId, headKey);
                            } catch (ObjectDoesntExistException e) {
                                headObj = linkListMakeNewHead(links.get(startTypeIndex), false);
                            }

                            logger.debug("addBulkLinks: current head of size " + headObj.getValueBytes().length + " Bytes");
                            
                            for (int k = startTypeIndex; k < j; k++) {
                                RAMCloudObject newTailSeg = linkListPrependToHead(links.get(k), headObj);

                                if (newTailSeg != null) {
                                    logger.debug("addBulkLinks: writing new tail of size " + newTailSeg.getValueBytes().length + " Bytes");
                                    tx.write(outLinksTableId, newTailSeg.getKeyBytes(), newTailSeg.getValueBytes());
                                }
                            }

                            logger.debug("addBulkLinks: new head of size " + headObj.getValueBytes().length + " Bytes");
                            
                            tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());

                            startTypeIndex = j;
                        }
                    }
                    
                    logger.debug("addBulkLinks: " + (i-startTypeIndex) + "/" + links.size() + " are for (id1=" + links.get(startTypeIndex).id1 + ", type=" + links.get(startTypeIndex).link_type + ")");      
                    // From startTypeIndex to i-1 are all links belonging to the 
                    // same Id1 and with the same type. These are at the end of 
                    // the list. 
                    byte[] headKey = linkListMakeHeadKey(links.get(startTypeIndex));

                    RAMCloudObject headObj;
                    try {
                        headObj = tx.read(outLinksTableId, headKey);
                    } catch (ObjectDoesntExistException e) {
                        headObj = linkListMakeNewHead(links.get(startTypeIndex), false);
                    }

                    logger.debug("addBulkLinks: current head of size " + headObj.getValueBytes().length + " Bytes");
                    
                    for (int k = startTypeIndex; k < i; k++) {
                        RAMCloudObject newTailSeg = linkListPrependToHead(links.get(k), headObj);

                        if (newTailSeg != null) {
                            logger.debug("addBulkLinks: writing new tail of size " + newTailSeg.getValueBytes().length + " Bytes");
                            tx.write(outLinksTableId, newTailSeg.getKeyBytes(), newTailSeg.getValueBytes());
                        }
                    }

                    logger.debug("addBulkLinks: new head of size " + headObj.getValueBytes().length + " Bytes");
                    
                    tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());

                            
                    startId1Index = i;
                }
            }
            
            // From startId1Index to links.size()-1 are all links belonging to 
            // the same Id1. 
            int startTypeIndex = startId1Index;
            for (int j = startTypeIndex + 1; j < links.size(); j++) {
                if (links.get(j).link_type != links.get(j - 1).link_type) {
                    logger.debug("addBulkLinks: " + (j-startTypeIndex) + "/" + links.size() + " are for (id1=" + links.get(startTypeIndex).id1 + ", type=" + links.get(startTypeIndex).link_type + ")");
                    // From startTypeIndex to j-1 are all links
                    // belonging to the same Id1 and with the same type
                    // We are going to take this range of links in this 
                    // list and bulk insert them 
                    byte[] headKey = linkListMakeHeadKey(links.get(startTypeIndex));

                    RAMCloudObject headObj;
                    try {
                        headObj = tx.read(outLinksTableId, headKey);
                    } catch (ObjectDoesntExistException e) {
                        headObj = linkListMakeNewHead(links.get(startTypeIndex), false);
                    }

                    logger.debug("addBulkLinks: current head of size " + headObj.getValueBytes().length + " Bytes");
                    
                    for (int k = startTypeIndex; k < j; k++) {
                        RAMCloudObject newTailSeg = linkListPrependToHead(links.get(k), headObj);

                        if (newTailSeg != null) {
                            logger.debug("addBulkLinks: writing new tail of size " + newTailSeg.getValueBytes().length + " Bytes");
                            tx.write(outLinksTableId, newTailSeg.getKeyBytes(), newTailSeg.getValueBytes());
                        }
                    }

                    logger.debug("addBulkLinks: new head of size " + headObj.getValueBytes().length + " Bytes");
                    
                    tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());

                    startTypeIndex = j;
                }
            }

            logger.debug("addBulkLinks: " + (links.size()-startTypeIndex) + "/" + links.size() + " are for (id1=" + links.get(startTypeIndex).id1 + ", type=" + links.get(startTypeIndex).link_type + ")");
            // From startTypeIndex to links.size()-1 are all links belonging to the 
            // same Id1 and with the same type. These are at the end of 
            // the list. 
            byte[] headKey = linkListMakeHeadKey(links.get(startTypeIndex));

            RAMCloudObject headObj;
            try {
                headObj = tx.read(outLinksTableId, headKey);
            } catch (ObjectDoesntExistException e) {
                headObj = linkListMakeNewHead(links.get(startTypeIndex), false);
            }

            logger.debug("addBulkLinks: current head of size " + headObj.getValueBytes().length + " Bytes");
            
            for (int k = startTypeIndex; k < links.size(); k++) {
                RAMCloudObject newTailSeg = linkListPrependToHead(links.get(k), headObj);

                if (newTailSeg != null) {
                    logger.debug("addBulkLinks: writing new tail of size " + newTailSeg.getValueBytes().length + " Bytes");
                    tx.write(outLinksTableId, newTailSeg.getKeyBytes(), newTailSeg.getValueBytes());
                }
            }

            logger.debug("addBulkLinks: new head of size " + headObj.getValueBytes().length + " Bytes");
            
            tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());

            if(tx.commitAndSync()) {
                outLinksTableValueSize += links.size()*LINK_HEADER_SIZE;
                for(Link link: links) 
                    outLinksTableValueSize += link.data.length;
                
                return;
            }
            
            txRetries++;
            
            if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("addBulkLinks: transaction failed " + txRetries + " times");
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
    }
  
    /**
     * Add a batch of counts.
     * 
     * This method does nothing in RAMCloud.
     */
    public void addBulkCounts(String dbid, List<LinkCount> a)
            throws Exception {
        return;
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        if(noinverse == false)
            throw new UnsupportedOperationException("Inverse not supported yet.");
        
        logger.debug("deleteLink: deleting link=(id=" + id1 + ", link_type=" + link_type + ", id2=" + id2 + ")");
        
        Link link = new Link(id1, link_type, id2, (byte)0, null, 0, 0);
        
        byte[] headKey = linkListMakeHeadKey(link);
        
        int txRetries = 0;
        while(txRetries < TX_MAX_RETRIES) {
            tx.clear();
            
            RAMCloudObject headObj;
            try {
                headObj = tx.read(outLinksTableId, headKey);
            } catch (ObjectDoesntExistException e) {
                return false;
            }

            byte[][] tailKeys = linkListMakeTailKeys(headObj);
            RAMCloudObject[] tailObjs = null;
            int tailSegments = 0;
            if (tailKeys != null) {
                tailSegments = tailKeys.length;
                tailObjs = new RAMCloudObject[tailSegments];
                for (int i = 0; i < tailSegments; i++) {
                    tailObjs[i] = tx.read(outLinksTableId, tailKeys[i]);
                }
            }

            LinkListOpCode op;
            if (expunge) {
                op = LinkListOpCode.EXPUNGE;
            } else {
                op = LinkListOpCode.HIDE;
            }

            int segIndex = linkListFindLink(headObj, tailObjs, link, op);

            if (segIndex != -1) {
                if (segIndex == tailSegments)
                    tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());
                else
                    tx.write(outLinksTableId, tailObjs[segIndex].getKeyBytes(), tailObjs[segIndex].getValueBytes());
            }
            
            if(tx.commitAndSync()) {
                if(segIndex != -1)
                    return true;
                else
                    return false;
            }
            
            txRetries++;
            
            if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("deleteLink: transaction failed " + txRetries + " times");
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
    }
    
    @Override
    public boolean updateLink(String dbid, Link newLink, boolean noinverse) throws Exception {
        if(noinverse == false)
            throw new UnsupportedOperationException("Inverse not supported yet.");
        
        logger.debug("updateLink: updating newLink=" + newLink.toString());
        
        byte[] headKey = linkListMakeHeadKey(newLink);
        
        int txRetries = 0;
        while(txRetries < TX_MAX_RETRIES) {
            tx.clear();
            
            RAMCloudObject headObj;
            try {
                headObj = tx.read(outLinksTableId, headKey);
            } catch (ObjectDoesntExistException e) {
                return false; // Didn't find the link
            }

            byte[][] tailKeys = linkListMakeTailKeys(headObj);
            RAMCloudObject[] tailObjs = null;
            int tailSegments = 0;
            if (tailKeys != null) {
                tailSegments = tailKeys.length;
                tailObjs = new RAMCloudObject[tailSegments];
                for (int i = 0; i < tailSegments; i++) {
                    tailObjs[i] = tx.read(outLinksTableId, tailKeys[i]);
                }
            }

            Link oldLink = newLink.clone();
            int oldLinkSegIndex = linkListFindLink(headObj, tailObjs, oldLink, LinkListOpCode.EXPUNGE);

            if (oldLinkSegIndex != -1) {
                int newLinkSegIndex = linkListInsert(headObj, tailObjs, newLink);

                if (newLinkSegIndex == -1) {
                    logger.error("updateLink(): linkListInsert returned -1");
                    throw new Exception();
                }

                if (newLinkSegIndex == tailSegments) {
                    RAMCloudObject newTailSeg = linkListEnforceHeadLimits(headObj);
                    if (newTailSeg != null) {
                        tx.write(outLinksTableId, newTailSeg.getKeyBytes(), newTailSeg.getValueBytes());
                    }
                }

                if (newLinkSegIndex == oldLinkSegIndex) {
                    if (newLinkSegIndex == tailSegments) {
                        tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());
                    } else {
                        tx.write(outLinksTableId, tailObjs[newLinkSegIndex].getKeyBytes(), tailObjs[newLinkSegIndex].getValueBytes());
                    }
                } else {
                    if (newLinkSegIndex == tailSegments) {
                        tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());
                    } else {
                        tx.write(outLinksTableId, tailObjs[newLinkSegIndex].getKeyBytes(), tailObjs[newLinkSegIndex].getValueBytes());
                    }

                    if (oldLinkSegIndex == tailSegments) {
                        tx.write(outLinksTableId, headObj.getKeyBytes(), headObj.getValueBytes());
                    } else {
                        tx.write(outLinksTableId, tailObjs[oldLinkSegIndex].getKeyBytes(), tailObjs[oldLinkSegIndex].getValueBytes());
                    }
                }
            }
            
            if(tx.commitAndSync()) {
                if(oldLinkSegIndex != -1)
                    return true; // Found it
                else
                    return false; // Didn't find it
            }
            
            txRetries++;
            
            if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("updateLink: transaction failed " + txRetries + " times");
            
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
    }
       
    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        logger.debug("getLink: getting link=(id=" + id1 + ", link_type=" + link_type + ", id2=" + id2 + ")");
        
        Link link = new Link(id1, link_type, id2, (byte)0, null, 0, 0);
        
        byte[] headKey = linkListMakeHeadKey(link);
        
        int txRetries = 0;
        while(txRetries < TX_MAX_RETRIES) {
            tx.clear();
            
            RAMCloudObject headObj;
            try {
                headObj = tx.read(outLinksTableId, headKey);
            } catch (ObjectDoesntExistException e) {
                return null;
            }

            byte[][] tailKeys = linkListMakeTailKeys(headObj);
            RAMCloudObject[] tailObjs = null;
            int tailSegments = 0;
            if (tailKeys != null) {
                tailSegments = tailKeys.length;
                tailObjs = new RAMCloudObject[tailSegments];
                for (int i = 0; i < tailSegments; i++) {
                    tailObjs[i] = tx.read(outLinksTableId, tailKeys[i]);
                }
            }

            int segIndex = linkListFindLink(headObj, tailObjs, link, LinkListOpCode.GET);

            if(tx.commitAndSync()) {
                if (segIndex != -1)
                    return link;
                else
                    return null;
            }
            
            txRetries++;
            
            if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("getLink: transaction failed " + txRetries + " times");
            
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        logger.debug("getLinkList(): id1=" + id1 + " link_type=" + link_type + " minTS=" + minTimestamp + " maxTS=" + maxTimestamp + " offset=" + offset + " limit=" + limit);
        Link link = new Link(id1, link_type, 0, (byte)0, null, 0, 0);
        byte[] headKey = linkListMakeHeadKey(link);
        
        int txRetries = 0;
        while(txRetries < TX_MAX_RETRIES) {
            tx.clear();
            
            RAMCloudObject headObj;
            try {
                headObj = tx.read(outLinksTableId, headKey);
            } catch (ObjectDoesntExistException e) {
                return null;
            }

            ArrayList<Link> linkArray = new ArrayList<Link>();
            SearchState state = new SearchState(offset, limit);

            if (parseLinkList(linkArray, headObj, true, maxTimestamp, minTimestamp, state)) {
                byte[][] tailKeys = linkListMakeTailKeys(headObj);
                if (tailKeys != null) {
                    int tailSegments = tailKeys.length;
                    for (int i = tailSegments - 1; i >= 0; i--) {
                        RAMCloudObject tailObj = tx.read(outLinksTableId, tailKeys[i]);
                        if (!parseLinkList(linkArray, tailObj, false, maxTimestamp, minTimestamp, state)) {
                            break;
                        }
                    }
                }
            }

            if(tx.commitAndSync()) {
                if (linkArray.isEmpty())
                    return null;

                Link[] returnArray = new Link[linkArray.size()];
                return linkArray.toArray(returnArray);
            }
            
            txRetries++;
            
            if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("getLinkList: transaction failed " + txRetries + " times");
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        
        logger.debug("countLinks: counting links from id1=" + id1 + " link_type=" + link_type);
        
        Link link = new Link(id1, link_type, 0, (byte)0, null, 0, 0);
        byte[] headKey = linkListMakeHeadKey(link);
        
        int txRetries = 0;
        while(txRetries < TX_MAX_RETRIES) {
            tx.clear();
            
            RAMCloudObject headObj;
            try {
                headObj = tx.read(outLinksTableId, headKey);
            } catch (ObjectDoesntExistException e) {
                return 0;
            }

            byte[][] tailKeys = linkListMakeTailKeys(headObj);
            RAMCloudObject[] tailObjs = null;
            int tailSegments = 0;
            if (tailKeys != null) {
                tailSegments = tailKeys.length;
                logger.debug("countLinks: reading " + tailSegments + " tail segments");
                tailObjs = new RAMCloudObject[tailSegments];
                for (int i = 0; i < tailSegments; i++) {
                    tailObjs[i] = tx.read(outLinksTableId, tailKeys[i]);
                }
            }

            if(tx.commitAndSync())
                return linkListCountLinks(headObj, tailObjs);
            
            txRetries++;
            
            if(txRetries%TX_FAIL_REPORT_COUNT == 0)
                logger.warn("countLinks: transaction failed " + txRetries + " times");
        }
        
        throw new Exception("Exceed maximum transaction retry count (" + TX_MAX_RETRIES + ")");
    }
}
