package eu.ows;

import io.javalin.Javalin;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the application.
 */
public class SearchApi {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchApi.class);

    /**
     * Handles the arguments and starts the server.
     * @param args[0] Default Lucene index.
     * @param args[1] Port for the server.
     * @param args[2] Path of directory containing the Lucene indexes (optional).
     * @param args[3] Path of directory containing the Parquet files (optional).
     */
    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder("d")
            .argName("index").longOpt("default-index")
            .hasArg()
            .desc("Default index used for queries")
            .build());
        options.addOption(Option.builder("p")
            .argName("port").longOpt("port")
            .hasArg()
            .desc("Port of the application")
            .build());
        options.addOption(Option.builder("l")
            .argName("dirPath").longOpt("lucene-dir-path")
            .hasArg()
            .desc("Path of directory containing the Lucene index(es)")
            .build());
        options.addOption(Option.builder("m")
            .argName("dirPath").longOpt("parquet-dir-path")
            .hasArg()
            .desc("Path of directory containing the Parquet file(s)")
            .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.err.println("Error: " + e.getMessage());
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("<executable or script>", options);
            System.exit(1);
        }

        String defaultIndex = cmd.getOptionValue("d");
        int port = Integer.parseInt(cmd.getOptionValue("p"));
        ApiUtils.setIndexDirPath(cmd.getOptionValue("l", ApiUtils.DEFAULT_INDEX_DIR_PATH));
        ApiUtils.setParquetDirPath(cmd.getOptionValue("m", ApiUtils.DEFAULT_PARQUET_DIR_PATH));

        Javalin app = Javalin.create(config -> {
            config.plugins.enableCors(cors -> {
                cors.add(it -> {
                    it.anyHost();
                });
            });
        });

        app.start(port);
        LOGGER.info("Running app on port {}", port);

        RequestHandler requestHandler = new RequestHandler();

        app.get("/search", ctx -> requestHandler.handleSearchRequest(ctx, defaultIndex));
        app.exception(Exception.class, (e, ctx) -> {
            LOGGER.warn("App exception: {}", e);
            ctx.status(500);
        });
    }
}
