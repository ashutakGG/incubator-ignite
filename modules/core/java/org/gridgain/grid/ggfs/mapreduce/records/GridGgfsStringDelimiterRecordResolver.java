// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.mapreduce.records;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.charset.*;

/**
 * Record resolver based on delimiters represented as strings. Works in the same way as
 * {@link GridGgfsByteDelimiterRecordResolver}, but uses strings as delimiters instead of byte arrays.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridGgfsStringDelimiterRecordResolver extends GridGgfsByteDelimiterRecordResolver {
    /**
     * Converts string delimiters to byte delimiters.
     *
     * @param charset Charset.
     * @param delims String delimiters.
     * @return Byte delimiters.
     */
    @Nullable private static byte[][] toBytes(Charset charset, @Nullable String... delims) {
        byte[][] res = null;

        if (delims != null) {
            res = new byte[delims.length][];

            for (int i = 0; i < delims.length; i++)
                res[i] = delims[i].getBytes(charset);
        }

        return res;
    }

    /**
     * Empty constructor required for {@link Externalizable} support.
     */
    public GridGgfsStringDelimiterRecordResolver() {
        // No-op.
    }

    /**
     * Creates record resolver from given string and given charset.
     *
     * @param delims Delimiters.
     * @param charset Charset.
     */
    public GridGgfsStringDelimiterRecordResolver(Charset charset, String... delims) {
        super(toBytes(charset, delims));
    }

    /**
     * Creates record resolver based on given string with default charset.
     *
     * @param delims Delimiters.
     */
    public GridGgfsStringDelimiterRecordResolver(String... delims) {
        super(toBytes(Charset.defaultCharset(), delims));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsStringDelimiterRecordResolver.class, this);
    }
}
