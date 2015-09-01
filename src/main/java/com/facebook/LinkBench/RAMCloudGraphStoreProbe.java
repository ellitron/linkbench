/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.facebook.LinkBench;

import static com.facebook.LinkBench.LinkBenchDriver.EXIT_BADARGS;
import static com.facebook.LinkBench.LinkBenchDriver.EXIT_BADCONFIG;
import com.facebook.LinkBench.stats.SampledStats;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;
import edu.stanford.ramcloud.multiop.MultiReadObject;
import edu.stanford.ramcloud.transactions.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;

import org.apache.log4j.Logger;

/**
 *
 * @author ellitron
 */
public class RAMCloudGraphStoreProbe  {

    /* Command line arguments */
    private static String configFile = null;
    private static long id1;
    
    private static Properties props;
    
    public RAMCloudGraphStoreProbe() {
    }
    
    public static void main(String[] args)
            throws IOException, InterruptedException, Throwable {
        processArgs(args);
        
        props = new Properties();
        props.load(new FileInputStream(configFile));
        
        RAMCloudGraphStore store = new RAMCloudGraphStore();
        store.initialize(props, Phase.REQUEST, 0); 
        
        String dbid = ConfigUtil.getPropertyRequired(props, Config.DBID);
        
        Node node = store.getNode(dbid, LinkStore.DEFAULT_NODE_TYPE, id1);
        System.out.println("Node(" + "id=" + node.id + ",type=" + node.type + ",version=" + node.version + ","
                   + "timestamp=" + node.time + ",dataLength="
                   + node.data.length + ")");
        
        int linkTypeCount = ConfigUtil.getInt(props, Config.LINK_TYPE_COUNT, 1);
        
        for (int i = 0; i < linkTypeCount; i++) {
            long linkCount = store.countLinks(dbid, id1, LinkStore.DEFAULT_LINK_TYPE + i);
            System.out.println("Link Count (type=" + (LinkStore.DEFAULT_LINK_TYPE + i) + ") = " + linkCount);
            
            Link[] links = store.getLinkList(dbid, id1, LinkStore.DEFAULT_LINK_TYPE + i);
            if(links != null) {
                for (int j = 0; j < links.length; j++) {
                    System.out.println(String.format("Link %d (id1=%d, id2=%d, link_type=%d, "
                            + "visibility=%d, version=%d, "
                            + "time=%d, dataLength=%d)", j, links[j].id1, links[j].id2, links[j].link_type,
                            links[j].visibility, links[j].version, links[j].time, links[j].data.length));
                }
            }
        }
        
        store.close();
    }

    private static void printUsage(Options options) {
        //PrintWriter writer = new PrintWriter(System.err);
        HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp("ramcloudgraphstoreprobe", options, true);
    }

    private static Options initializeOptions() {
        Options options = new Options();
        Option config = new Option("c", true, "RAMCloud linkbench config file");
        config.setArgName("file");
        options.addOption(config);

        Option id1 = new Option("id1", true, "id1 to probe");
        options.addOption(id1);
        
        return options;
    }

    /**
     * Process command line arguments and set static variables exits program if
     * invalid arguments provided
     *
     * @param options
     * @param args
     * @throws ParseException
     */
    private static void processArgs(String[] args)
            throws ParseException {
        Options options = initializeOptions();

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new GnuParser();
            cmd = parser.parse(options, args);
        } catch (ParseException ex) {
            // Use Apache CLI-provided messages
            System.err.println(ex.getMessage());
            printUsage(options);
            System.exit(EXIT_BADARGS);
        }

        /*
         * Apache CLI validates arguments, so can now assume
         * all required options are present, etc
         */
        if (cmd.getArgs().length > 0) {
            System.err.print("Invalid trailing arguments:");
            for (String arg : cmd.getArgs()) {
                System.err.print(' ');
                System.err.print(arg);
            }
            System.err.println();
            printUsage(options);
            System.exit(EXIT_BADARGS);
        }
        
        configFile = cmd.getOptionValue('c');
        
        id1 = Long.parseLong(cmd.getOptionValue("id1"));
    }
}
