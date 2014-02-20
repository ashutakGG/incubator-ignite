// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#ifndef GRID_CLIENT_PROTOCOL_HPP_INCLUDED
#define GRID_CLIENT_PROTOCOL_HPP_INCLUDED

/**
 * Protocol that will be used when a client connections are created.
 */
enum GridClientProtocol {
    /** Communication via HTTP protocol. */
    HTTP,

    /** Communication via tcp binary protocol. */
    TCP
};

#endif