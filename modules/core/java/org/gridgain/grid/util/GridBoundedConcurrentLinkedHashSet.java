// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

import static org.gridgain.grid.util.ConcurrentLinkedHashMap.*;
import static org.gridgain.grid.util.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 * Concurrent set with an upper bound. Once set reaches its maximum capacity,
 * the eldest elements will be removed based on insertion or access order.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridBoundedConcurrentLinkedHashSet<E> extends GridSetWrapper<E> {
    /**
     * Creates a new, empty set with specified order and default initial capacity (16),
     * load factor (0.75) and concurrencyLevel (16).
     *
     * @param max Upper bound of this set.
     */
    public GridBoundedConcurrentLinkedHashSet(int max) {
        this(max, DFLT_INIT_CAP, DFLT_LOAD_FACTOR, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty set with the specified initial capacity,
     * and with default load factor (0.75) and concurrencyLevel (16).
     *
     * @param max Upper bound of this set.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative.
     */
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap) {
        this(max, initCap, DFLT_LOAD_FACTOR, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty set with the specified initial capacity
     * and load factor and with the default concurrencyLevel (16).
     *
     * @param max Upper bound of this set.
     * @param initCap The implementation performs internal
     *      sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative or the load factor is nonpositive.
     */
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap, float loadFactor) {
        this(max, initCap, loadFactor, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param max Upper bound of this set.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param concurLvl the estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurLvl are
     *      nonpositive.
     */
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap, float loadFactor, int concurLvl) {
        this(max, initCap, loadFactor, concurLvl, SINGLE_Q);
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, load factor and concurrency level.
     *
     *
     * @param max Upper bound of this set.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param concurLvl the estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @param qPlc Queue policy.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurLvl are
     *      nonpositive.
     */
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap, float loadFactor, int concurLvl, QueuePolicy qPlc) {
        super(new GridBoundedConcurrentLinkedHashMap<E, Object>(max, initCap, loadFactor, concurLvl, qPlc));
    }

    /**
     * Note that unlike regular add operation on a set, this method will only
     * add the passed in element if it's not already present in set.
     *
     * @param e Element to add.
     * @return {@code True} if element was added.
     */
    @Override public boolean add(E e) {
        ConcurrentMap<E, Object> m = (ConcurrentMap<E, Object>)map;

        return m.putIfAbsent(e, e) == null;
    }

    /**
     * Note that unlike regular add operation on a set, this method will only
     * add the passed in element if it's not already present in set.
     *
     * @param e Element to add.
     * @return Value previously present in set or {@code null} if set didn't have this value.
     */
    @Nullable public E addx(E e) {
        ConcurrentMap<E, Object> map0 = map();

        return (E)map0.putIfAbsent(e, e);
    }

    /**
     * @return Size of the set
     */
    public int sizex() {
        return ((ConcurrentLinkedHashMap<E, Object>)map()).sizex();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        // TODO GG-4788
        return ((ConcurrentLinkedHashMap<E, Object>)map()).policy() != SINGLE_Q ?
            S.toString(GridBoundedConcurrentLinkedHashSet.class, this) :
            S.toString(GridBoundedConcurrentLinkedHashSet.class, this, "elements", map().keySet());
    }
}
