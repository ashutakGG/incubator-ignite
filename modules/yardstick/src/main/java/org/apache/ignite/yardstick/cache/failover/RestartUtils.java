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

package org.apache.ignite.yardstick.cache.failover;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Restart Utils.
 */
public final class RestartUtils {
    /** Time formatter. */
    public static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("HHmmss");

    /**
     * Private default constructor.
     */
    private RestartUtils() {
        // No-op.
    }

    /**
     * Kills 'Dyardstick.server${ID}' process with -9 option on remote host.
     *
     * @param cfg Benchmark configuration.
     * @param isDebug Is debug flag. If <code>true</code> then result will contain 'out' and 'err' streams output.
     * But it will affect a performance.
     * @return Result of execution.
     */
    public static Result kill9Server(BenchmarkConfiguration cfg, boolean isDebug) {
        return executeUnderSshConnect(cfg.remoteUser(), cfg.remoteHostName(), isDebug,
            Collections.singletonList("pkill -9 -f 'Dyardstick.server" + cfg.memberId() + "'"));
    }

    /**
     * Runs process with ssh connection and execute commands under ssh.
     *
     * @param remoteUser Remote user.
     * @param hostName Host name.
     * @param isDebug Is debug flag.
     * @param cmds Commands.
     * @return Result.
     */
    private static Result executeUnderSshConnect(String remoteUser, String hostName,
        boolean isDebug, Iterable<String> cmds) {
        if (U.isWindows())
            throw new UnsupportedOperationException("Unsupported operation for windows.");

        Tuple<Thread, StringBuffer> t1 = null;
        Tuple<Thread, StringBuffer> t2 = null;

        try {
            StringBuilder log = new StringBuilder("RemoteUser=" + remoteUser + ", hostName=" + hostName).append('\n');

            Process p = Runtime.getRuntime().exec("ssh -o PasswordAuthentication=no " + remoteUser + "@" + hostName);

            try(PrintStream out = new PrintStream(p.getOutputStream(), true)) {
                if (isDebug) {
                    t1 = monitorInputeStream(p.getInputStream(), "OUT");
                    t2 = monitorInputeStream(p.getErrorStream(), "ERR");
                }

                for (String cmd : cmds) {
                    log.append("Executing cmd=").append(cmd).append('\n');

                    out.println(cmd);
                }

                out.println("exit");

                p.waitFor();
            }

            return new Result(p.exitValue(), log.toString(),  t1 == null ? "" : t1.val2.toString(),
                t2 == null ? "" : t2.val2.toString());
        }
        catch (Exception err) {
            return new Result(err);
        }
        finally {
            if (isDebug) {
                if (t1 != null && t1.val1 != null)
                    t1.val1.interrupt();

                if (t2 != null && t2.val1 != null)
                    t2.val1.interrupt();
            }
        }
    }

    /**
     * Monitors input stream at separate thread.
     *
     * @param in Input stream.
     * @param name Name.
     * @return Tuple of started thread and output string.
     */
    private static Tuple<Thread, StringBuffer> monitorInputeStream(final InputStream in, final String name) {
        final StringBuffer sb = new StringBuffer();

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try (BufferedReader input = new BufferedReader(new InputStreamReader(in))) {
                    String l;

                    while (!Thread.currentThread().isInterrupted() && ((l = input.readLine()) != null))
                        sb.append(l).append('\n');
                }
                catch (IOException e) {
                    sb.append(e).append('\n');
                }
            }
        }, name);

        thread.setDaemon(true);

        thread.start();

        return new Tuple<>(thread, sb);
    }

    /**
     * Starts server process on remote host.
     *
     * @param cfg Benchmark configuration.
     * @param isDebug Is debug flag. If <code>true</code> then result will contain 'out' and 'err' streams output.
     * But it will affect a performance.
     * @return Result of execution.
     */
    public static Result startServer(BenchmarkConfiguration cfg,
        boolean isDebug) {
        String descriptrion = F.isEmpty(cfg.descriptions()) ? "" : "_" + cfg.descriptions().get(0);
        String now = TIME_FORMATTER.format(new Date());

        String logFile = cfg.logsFolder() +"/"+ now + "_id" + cfg.memberId() + "_" + cfg.remoteHostName()
            + descriptrion + ".log";

        StringBuilder cmdArgs = new StringBuilder();

        for (String arg : cfg.commandLineArguments())
            cmdArgs.append(arg).append(' ');

        String java = cfg.customProperties().get("JAVA");

        Collection<String> cmds = new ArrayList<>();

        if (!F.isEmpty(cfg.currentFolder()))
            cmds.add("cd " + cfg.currentFolder());

        cmds.add("nohup "
            + java + " " + cfg.customProperties().get("JVM_OPTS")
            + " -cp " + cfg.customProperties().get("CLASSPATH")
            + " org.yardstickframework.BenchmarkServerStartUp " + cmdArgs + " > " + logFile + " 2>& 1 &");

        return executeUnderSshConnect(cfg.remoteUser(), cfg.remoteHostName(), isDebug, cmds);
    }

    /**
     * Result of executed command.
     */
    public static class Result {
        /** */
        private final int exitCode;

        /** */
        private final String log;

        /** Contant of output stream of the process. */
        private final String out;

        /** Contant of error output stream of the process. */
        private final String err;

        /** */
        private final Exception e;

        /**
         * @param exitCode Exit code.
         * @param out Out.
         * @param err Err.
         */
        public Result(int exitCode, String log, String out, String err) {
            this.exitCode = exitCode;
            this.log = log;
            this.out = out;
            this.err = err;

            e = null;
        }

        /**
         * @param e Exception.
         */
        public Result(Exception e) {
            exitCode = -1;
            out = null;
            err = null;
            log = null;

            this.e = e;
        }

        /**
         * @return Exit code.
         */
        public int getExitCode() {
            return exitCode;
        }

        /**
         * @return Execution log.
         */
        public String getLog() {
            return log;
        }

        /**
         * @return Output of ssh process.
         */
        public String getOutput() {
            return out;
        }

        /**
         * @return Error output of ssh process.
         */
        public String getErrorOutput() {
            return err;
        }

        /**
         * @return Exception.
         */
        public Exception getException() {
            return e;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Result{\n" +
                "exitCode=" + exitCode + '\n' +
                "log=\n" + log + '\n' +
                "out=\n'" + out + '\n' +
                "err=\n'" + err + '\n' +
                "e=" + e +
                '}';
        }
    }

    /**
     * Tuple.
     *
     * @param <T> Type of first element.
     * @param <K> Type of second element.
     */
    private static class Tuple<T, K> {
        /** */
        private T val1;

        /** */
        private K val2;

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        Tuple(T val1, K val2) {
            this.val1 = val1;
            this.val2 = val2;
        }
    }
}
