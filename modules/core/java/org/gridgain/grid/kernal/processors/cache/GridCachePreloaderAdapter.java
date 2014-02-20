// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;

import java.util.*;

/**
 * Adapter for preloading which always assumes that preloading finished.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCachePreloaderAdapter<K, V> implements GridCachePreloader<K, V> {
    /** Cache context. */
    protected final GridCacheContext<K, V> cctx;

    /** Logger.*/
    protected final GridLogger log;

    /** Affinity. */
    protected final GridCacheAffinity aff;

    /** Start future (always completed by default). */
    private final GridFuture finFut;

    /** Preload predicate. */
    protected GridPredicate<GridCacheEntryInfo<K, V>> preloadPred;

    /**
     * @param cctx Cache context.
     */
    public GridCachePreloaderAdapter(GridCacheContext<K, V> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        log = cctx.logger(getClass());
        aff = cctx.config().getAffinity();

        finFut = new GridFinishedFuture(cctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void forcePreload() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void preloadPredicate(GridPredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /** {@inheritDoc} */
    @Override public GridPredicate<GridCacheEntryInfo<K, V>> preloadPredicate() {
        return preloadPred;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> startFuture() {
        return finFut;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> syncFuture() {
        return finFut;
    }

    /** {@inheritDoc} */
    @Override public void unwindUndeploys() {
        cctx.deploy().unwind();
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Object> request(Collection<? extends K> keys, long topVer) {
        return new GridFinishedFuture<>(cctx.kernalContext());
    }
}
