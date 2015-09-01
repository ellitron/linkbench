/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.facebook.LinkBench;

import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author ellitron
 */
public class RAMCloudLinkStoreTest extends LinkStoreTestBase {

    /**
     * Properties for last initStore call
     */
    private Properties currProps;

    @Override
    protected long getIDCount() {
        // Make test smaller so that it doesn't take too long
        return 1000;
    }

    @Override
    protected int getRequestCount() {
        // Fewer requests to keep test quick
        return 10000;
    }

    protected Properties basicProps() {
        Properties props = super.basicProps();
        RAMCloudTestConfig.fillRAMCloudTestProps(props);
        return props;
    }

    @Override
    protected void initStore(Properties props) throws IOException, Exception {
        this.currProps = (Properties) props.clone();
    }

    @Override
    public DummyLinkStore getStoreHandle(boolean initialize) throws IOException, Exception {
        DummyLinkStore result = new DummyLinkStore(new RAMCloudGraphStore());
        if (initialize) {
            result.initialize(currProps, Phase.REQUEST, 0);
        }
        return result;
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        RAMCloudGraphStore store = new RAMCloudGraphStore();
        store.initialize(currProps, Phase.LOAD, 0);
        store.dropTables();
        store.close();
    }
}
