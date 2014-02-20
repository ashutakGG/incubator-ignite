// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.ggfs.mapreduce.records.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Example that shows how to use {@link GridGgfsTask} to find lines matching particular pattern in the file in pretty
 * the same way as {@code grep} command does.
 * <p>
 * To start remote node, you can run {@link GridGgfsNoEndpointNodeStartup} class.
 * <p>
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code './ggstart.sh examples/config/example-ggfs-no-endpoint.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(HADOOP)
public class GridGgfsMapReduceExample {
    /**
     * Runs example.
     *
     * @param args Command line arguments. First argument is file name, second argument is regex to look for.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            print("Please provide file name and regular expression.");
        else if (args.length == 1)
            print("Please provide regular expression.");
        else {
            Grid g = GridGain.start("examples/config/example-ggfs-no-endpoint.xml");

            try {
                // Prepare arguments.
                String fileName = args[0];

                File file = new File(fileName);

                String regexStr = args[1];

                // Get an instance of GridGain File System.
                GridGgfs fs = g.ggfs("ggfs");

                // Working directory path.
                GridGgfsPath workDir = new GridGgfsPath("/examples/ggfs");

                // Write file to GGFS.
                GridGgfsPath fsPath = new GridGgfsPath(workDir, file.getName());

                writeFile(fs, fsPath, file);

                Collection<Line> lines = fs.execute(new GrepTask(), GridGgfsNewLineRecordResolver.NEW_LINE,
                    Collections.singleton(fsPath), false, regexStr);

                if (lines.isEmpty())
                    print("No lines were found.");
                else {
                    for (Line line : lines)
                        print(line.fileLine());
                }

            }
            finally {
                GridGain.stop(false);
            }
        }
    }

    /**
     * Write file to the GridGain file system.
     *
     * @param fs GridGain file system.
     * @param fsPath GridGain file system path.
     * @param file File to write.
     * @throws Exception In case of exception.
     */
    private static void writeFile(GridGgfs fs, GridGgfsPath fsPath, File file) throws Exception {
        print("Copying file to GGFS: " + file);

        GridGgfsOutputStream os = null;
        FileInputStream fis = null;

        try {
            os = fs.create(fsPath, true);

            fis = new FileInputStream(file);

            byte[] buf = new byte[2048];

            int read = fis.read(buf);

            while (read != -1) {
                os.write(buf, 0, read);

                read = fis.read(buf);
            }
        }
        finally {
            U.closeQuiet(os);
            U.closeQuiet(fis);
        }
    }

    /**
     * Print particular string.
     *
     * @param str String.
     */
    private static void print(String str) {
        System.out.println(">>> " + str);
    }

    /**
     * Grep task.
     */
    private static class GrepTask extends GridGgfsTask<String, Collection<Line>> {
        /** {@inheritDoc} */
        @Override public GridGgfsJob createJob(GridGgfsPath path, GridGgfsFileRange range,
            GridGgfsTaskArgs<String> args) throws GridException {
            return new GrepJob(args.userArgument());
        }

        /** {@inheritDoc} */
        @Override public Collection<Line> reduce(List<GridComputeJobResult> results) throws GridException {
            Collection<Line> lines = new TreeSet<>(new Comparator<Line>() {
                @Override public int compare(Line line1, Line line2) {
                    return line1.rangePosition() < line2.rangePosition() ? -1 :
                        line1.rangePosition() > line2.rangePosition() ? 1 : line1.lineIndex() - line2.lineIndex();
                }
            });

            for (GridComputeJobResult res : results) {
                if (res.getException() != null)
                    throw res.getException();

                Collection<Line> line = res.getData();

                if (line != null)
                    lines.addAll(line);
            }

            return lines;
        }
    }

    /**
     * Grep job.
     */
    private static class GrepJob extends GridGgfsInputStreamJobAdapter {
        /** Regex string. */
        private final String regex;

        /**
         * Constructor.
         *
         * @param regex Regex string.
         */
        private GrepJob(String regex) {
            this.regex = regex;
        }

        /**  {@inheritDoc} */
        @Override public Object execute(GridGgfs ggfs, GridGgfsRangeInputStream in) throws GridException, IOException {
            Collection<Line> res = null;

            long start = in.startOffset();

            try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                int ctr = 0;

                String line = br.readLine();

                while (line != null) {
                    if (line.matches(".*" + regex + ".*")) {
                        if (res == null)
                            res = new HashSet<>();

                        res.add(new Line(start, ctr++, line));
                    }

                    line = br.readLine();
                }
            }

            return res;
        }
    }

    /**
     * Single file line with it's position.
     */
    private static class Line {
        /** Line start position in the file. */
        private long rangePos;

        /** Matching line index within the range. */
        private final int lineIdx;

        /** File line. */
        private String line;

        /**
         * Constructor.
         *
         * @param rangePos Range position.
         * @param lineIdx Matching line index within the range.
         * @param line File line.
         */
        private Line(long rangePos, int lineIdx, String line) {
            this.rangePos = rangePos;
            this.lineIdx = lineIdx;
            this.line = line;
        }

        /**
         * @return Range position.
         */
        public long rangePosition() {
            return rangePos;
        }

        /**
         * @return Matching line index within the range.
         */
        public int lineIndex() {
            return lineIdx;
        }

        /**
         * @return File line.
         */
        public String fileLine() {
            return line;
        }
    }
}
