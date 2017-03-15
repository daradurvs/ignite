package org.apache.ignite.cache.spring;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link SpringCache}
 */
public class SpringCacheTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite;

    /** */
    private static String gridName;

    /** Wrapped cache. */
    private IgniteCache nativeCache;

    /** Working cache. */
    private SpringCache springCache;

    /** */
    private String cacheName;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        gridName = getTestGridName();
        ignite = startGrid(gridName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        G.stop(gridName, true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cacheName = String.valueOf(System.currentTimeMillis());
        nativeCache = ignite.getOrCreateCache(cacheName);
        springCache = new SpringCache(nativeCache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ignite.destroyCache(cacheName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetName() throws Exception {
        assertEquals(cacheName, springCache.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetNativeCache() throws Exception {
        assertEquals(nativeCache, springCache.getNativeCache());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetByKey() throws Exception {
        String key = "key";
        String value = "value";

        springCache.put(key, value);
        assertEquals(value, springCache.get(key).get());

        assertNull(springCache.get("wrongKey"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetByKeyType() throws Exception {
        String key = "key";
        String value = "value";

        springCache.put(key, value);
        assertEquals(value, springCache.get(key, String.class));

        try {
            springCache.get(key, Integer.class);
            fail("Missing exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Cached value is not of required type [cacheName=" + cacheName));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        String key = "key";
        assertNull(springCache.get(key));

        String value = "value";
        springCache.put(key, value);

        assertEquals(value, springCache.get(key).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsent() throws Exception {
        String key = "key";
        String expected = "value";

        assertNull(springCache.putIfAbsent(key, expected));

        assertEquals(expected, springCache.putIfAbsent(key, "wrongValue").get());

        assertEquals(expected, springCache.get(key).get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvict() throws Exception {
        String key = "key";
        assertNull(springCache.get(key));

        springCache.put(key, "value");
        assertNotNull(springCache.get(key));

        springCache.evict(key);
        assertNull(springCache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClear() throws Exception {
        String key;
        springCache.put((key = "key1"), "value1");
        assertNotNull(springCache.get(key));
        springCache.put((key = "key2"), "value2");
        assertNotNull(springCache.get(key));
        springCache.put((key = "key3"), "value3");
        assertNotNull(springCache.get(key));

        springCache.clear();

        assertNull(springCache.get("key1"));
        assertNull(springCache.get("key2"));
        assertNull(springCache.get("key3"));
    }
}