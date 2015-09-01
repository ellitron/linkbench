/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.facebook.LinkBench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * @author ellitron
 */
public class RAMCloudTestConfig {
    // Hardcoded parameters for now
    static String coordinator_locator = "infrc:host=192.168.1.151,port=12246";
    static String idtable = "id_table";
    static String nodetable = "node_table";
    static String outlinkstable = "out_links_table";
    static String linklist_headsizethreshold = "1024";
    static String linklist_stumpsize = "512";
    static String master_servers = "1";
    
    public static void fillRAMCloudTestProps(Properties props) {
        props.setProperty(Config.LINKSTORE_CLASS, RAMCloudGraphStore.class.getName());
        props.setProperty(Config.NODESTORE_CLASS, RAMCloudGraphStore.class.getName());
        props.setProperty(RAMCloudGraphStore.CONFIG_COORD_LOC, coordinator_locator);
        props.setProperty(RAMCloudGraphStore.CONFIG_ID_TABLE, idtable);
        props.setProperty(RAMCloudGraphStore.CONFIG_OUTLINKS_TABLE, outlinkstable);
        props.setProperty(Config.NODE_TABLE, nodetable);
        props.setProperty(RAMCloudGraphStore.CONFIG_LINKLIST_HEADSIZETHRESHOLD, linklist_headsizethreshold);
        props.setProperty(RAMCloudGraphStore.CONFIG_LINKLIST_STUMPSIZE, linklist_stumpsize);
        props.setProperty(RAMCloudGraphStore.CONFIG_MASTER_SERVERS, master_servers);
    }
}
