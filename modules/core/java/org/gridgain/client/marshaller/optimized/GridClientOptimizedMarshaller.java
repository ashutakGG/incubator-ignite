// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.optimized;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.marshaller.optimized.*;

import java.io.*;
import java.util.*;

/**
 * Wrapper, that adapts {@link GridOptimizedMarshaller} to
 * {@link GridClientMarshaller} interface.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridClientOptimizedMarshaller implements GridClientMarshaller {
    /** Unique marshaller protocol ID. */
    public static final Byte PROTOCOL_ID = 1;

    /** Optimized marshaller. */
    private final GridOptimizedMarshaller opMarsh;

    /**
     * Default constructor.
     */
    public GridClientOptimizedMarshaller() {
        opMarsh = new GridOptimizedMarshaller();
    }

    /**
     * Constructs optimized marshaller with specific parameters.
     *
     * @param requireSer Flag to enforce {@link Serializable} interface or not. If {@code true},
     *      then objects will be required to implement {@link Serializable} in order to be
     *      marshalled, if {@code false}, then such requirement will be relaxed.
     * @param clsNames User preregistered class names.
     * @param clsNamesPath Path to a file with user preregistered class names.
     * @param poolSize Object streams pool size.
     * @throws IOException If an I/O error occurs while writing stream header.
     * @throws GridRuntimeException If this marshaller is not supported on the current JVM.
     * @see GridOptimizedMarshaller
     */
    public GridClientOptimizedMarshaller(boolean requireSer, List<String> clsNames, String clsNamesPath, int poolSize)
        throws IOException {
        try {
            opMarsh = new GridOptimizedMarshaller(requireSer, clsNames, clsNamesPath, poolSize);
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(Object obj) throws IOException {
        try {
            if (!(obj instanceof GridClientMessage))
                throw new IOException("Message serialization of given type is not supported: " +
                    obj.getClass().getName());

            return opMarsh.marshal(obj);
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        try {
            return opMarsh.unmarshal(bytes, null);
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolId() {
        return PROTOCOL_ID;
    }
}
