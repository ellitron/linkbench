/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.facebook.LinkBench;

import com.facebook.LinkBench.testtypes.RAMCloudTest;
import java.io.IOException;
import java.util.Properties;
import org.junit.experimental.categories.Category;

/**
 *
 * @author ellitron
 */
public class RAMCloudNodeStoreTest extends NodeStoreTestBase {
    
    Properties currProps;
    
    @Override
    protected Properties basicProps() {
        Properties props = super.basicProps();
        RAMCloudTestConfig.fillRAMCloudTestProps(props);
        return props;
    }
  
    @Override
    protected void initNodeStore(Properties props) throws Exception, IOException {
        currProps = (Properties) props.clone();
    }

    @Override
    protected NodeStore getNodeStoreHandle(boolean initialize) throws Exception, IOException {
        DummyLinkStore result = new DummyLinkStore(new RAMCloudGraphStore());
        if (initialize) {
            result.initialize(currProps, Phase.REQUEST, 0);
        }
        return result;
    }
    
}
