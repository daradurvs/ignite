/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.database.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.query.h2.database.DataStore;

/**
 * Abstract IO routines for B+Tree pages.
 */
public abstract class BPlusIO<L> extends PageIO {
    /** */
    protected static final int CNT_OFF = COMMON_HEADER_END;

    /** */
    protected static final int FORWARD_OFF = CNT_OFF + 2;

    /** */
    protected static final int REMOVE_ID_OFF = FORWARD_OFF + 8;

    /** */
    protected static final int ITEMS_OFF = REMOVE_ID_OFF + 8;

    /**
     * @param ver Page format version.
     */
    protected BPlusIO(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setCount(buf, 0);
        setForward(buf, 0);
        setRemoveId(buf, 0);
    }

    /**
     * @param buf Buffer.
     * @return Forward page ID.
     */
    public long getForward(ByteBuffer buf) {
        return buf.getLong(FORWARD_OFF);
    }

    /**
     * @param buf Buffer.
     * @param pageId Forward page ID.
     */
    public void setForward(ByteBuffer buf, long pageId) {
        buf.putLong(FORWARD_OFF, pageId);

        assert getForward(buf) == pageId;
    }

    /**
     * @param buf Buffer.
     * @return Remove ID.
     */
    public long getRemoveId(ByteBuffer buf) {
        return buf.getLong(REMOVE_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param rmvId Remove ID.
     */
    public void setRemoveId(ByteBuffer buf, long rmvId) {
        buf.putLong(REMOVE_ID_OFF, rmvId);

        assert getRemoveId(buf) == rmvId;
    }

    /**
     * @param buf Buffer.
     * @return Items count in the page.
     */
    public int getCount(ByteBuffer buf) {
        int cnt = buf.getShort(CNT_OFF) & 0xFFFF;

        assert cnt >= 0: cnt;

        return cnt;
    }

    /**
     * @param buf Buffer.
     * @param cnt Count.
     */
    public void setCount(ByteBuffer buf, int cnt) {
        assert cnt >= 0: cnt;

        buf.putShort(CNT_OFF, (short)cnt);

        assert getCount(buf) == cnt;
    }

    /**
     * @return {@code true} if it is a leaf page.
     */
    public abstract boolean isLeaf();

    /**
     * @param buf Buffer.
     * @return Max items count.
     */
    public abstract int getMaxCount(ByteBuffer buf);

    /**
     * Store the needed info about the row in the page. Leaf and inner pages can store different info.
     *
     * @param buf Buffer.
     * @param idx Index.
     * @param row Lookup or full row.
     */
    public abstract void store(ByteBuffer buf, int idx, L row);

    /**
     * Store row info from the given source.
     *
     * @param dst Destination buffer
     * @param dstIdx Destination index.
     * @param srcIo Source IO.
     * @param src Source buffer.
     * @param srcIdx Source index.
     */
    public abstract void store(ByteBuffer dst, int dstIdx, BPlusIO<L> srcIo, ByteBuffer src, int srcIdx);

    /**
     * @return {@code true} If we can get the whole row from this page using {@link DataStore}.
     * Must always be {@code true} for leaf pages.
     */
    public abstract boolean canGetRow();

    /**
     * Copy items from source buffer to destination buffer. Both pages must be of the same type.
     *
     * @param src Source buffer.
     * @param dst Destination buffer.
     * @param srcIdx Source begin index.
     * @param dstIdx Destination begin index.
     * @param cnt Items count.
     * @param cpLeft Copy leftmost link (makes sense only for inner pages).
     */
    public abstract void copyItems(ByteBuffer src, ByteBuffer dst, int srcIdx, int dstIdx, int cnt, boolean cpLeft);
}
