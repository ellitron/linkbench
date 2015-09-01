/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.facebook.LinkBench;

import edu.stanford.ramcloud.*;
import java.nio.ByteBuffer;

/**
 * This class encapsulates the details of the LinkBench Node object storage format 
 * in RAMCloud. It contains static methods for constructing a RAMCloudObject 
 * from a LinkBench Node, or a LinkBench Node from a RAMCloudObject that's been 
 * read from the
 * 
 * @author ellitron
 */
public class RAMCloudNodeObject  {
    Node node;
    RAMCloudObject obj;
    
    RAMCloudNodeObject(Node node) {
        this.node = node;
        ByteBuffer key = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
        key.putLong(node.id);
        
        int size = (Integer.SIZE + Long.SIZE + Integer.SIZE) / Byte.SIZE + node.data.length;
        ByteBuffer value = ByteBuffer.allocate(size);
    }
    
    
    
    
}
