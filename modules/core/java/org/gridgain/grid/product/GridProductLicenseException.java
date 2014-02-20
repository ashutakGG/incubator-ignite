// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.product;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * This exception is thrown when license violation is detected.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridProductLicenseException extends GridException {
    /** Short message. */
    private final String shortMsg;

    /**
     * Creates new license exception with given error message.
     *
     * @param msg Error message.
     * @param shortMsg Short error message presentable to the user. Note it should contain just letter and dot.
     */
    public GridProductLicenseException(String msg, @Nullable String shortMsg) {
        super(msg);

        this.shortMsg = shortMsg;
    }

    /**
     * Creates new license exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param shortMsg Short error message presentable to the user. Note it should contain just letter and dot.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridProductLicenseException(String msg, @Nullable String shortMsg, @Nullable Throwable cause) {
        super(msg, cause);

        this.shortMsg = shortMsg;
    }

    /**
     * @return shortMessage Short error message presentable to the user. Note it should contain just letter and dot.
     */
    public final String shortMessage() {
        return shortMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridProductLicenseException.class, this, "msg", getMessage(), "shortMsg", shortMsg);
    }
}
