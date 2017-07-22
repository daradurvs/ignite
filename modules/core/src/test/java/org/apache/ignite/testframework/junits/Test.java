package org.apache.ignite.testframework.junits;

import java.util.ArrayList;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class Test extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** */
    public void testFile() throws Exception {
        startGrid("testMultiVersion", null, "2.0.0", new ArrayList<String>());
//        startGrid(1);
    }
}
