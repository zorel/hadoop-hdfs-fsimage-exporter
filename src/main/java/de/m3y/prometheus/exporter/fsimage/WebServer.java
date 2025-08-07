package de.m3y.prometheus.exporter.fsimage;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageLoader;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Info;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.net.InetSocketAddress;

import static org.apache.log4j.Logger.getRootLogger;

public class WebServer {
    private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);

    static class HTTPServerWithCustomHandler extends HTTPServer {

        HTTPServerWithCustomHandler(InetSocketAddress addr) throws IOException {
            super(addr, CollectorRegistry.defaultRegistry, true);
        }

        void replaceRootHandler(ConfigHttpHandler configHttpHandler) {
            server.removeContext("/");
            server.createContext("/", configHttpHandler);
        }
    }

    private HTTPServerWithCustomHandler httpServer;
    private FsImageCollector fsImageCollector;
    private final Info buildInfo = Info.build()
            .name("fsimage_exporter_build")
            .help("Hadoop FSImage exporter build info")
            .labelNames("appVersion", "buildTime", "buildScmVersion", "buildScmBranch").create();


    WebServer configure(Config config, String address, int port) throws IOException {
        // Exporter own JVM metrics
        DefaultExports.initialize();

        // Build info
        buildInfo.labels(
                BuildMetaInfo.INSTANCE.getVersion(),
                BuildMetaInfo.INSTANCE.getBuildTimeStamp(),
                BuildMetaInfo.INSTANCE.getBuildScmVersion(),
                BuildMetaInfo.INSTANCE.getBuildScmBranch()
        );
        buildInfo.register();

        // Configure HTTP server
        InetSocketAddress inetAddress = new InetSocketAddress(address, port);
        httpServer = new HTTPServerWithCustomHandler(inetAddress);
        httpServer.replaceRootHandler(new ConfigHttpHandler(config));
        LOG.info("FSImage exporter started and listening on http://{}:{}", inetAddress.getHostName(), inetAddress.getPort());

        // Waits for parsed fsimage, so should run last after started HTTP server
        fsImageCollector = new FsImageCollector(config);
        fsImageCollector.register();

        return this;
    }

    public void stop() {
        httpServer.close();
        fsImageCollector.shutdown();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 && args.length != 3) {
            System.err.println("Usage: WebServer [-Dlog.level=[WARN|INFO|DEBUG]] (<hostname> <port> <yml configuration file>) | <yml configuration file>"); // NOSONAR
            System.exit(1);
        }

        getRootLogger().setLevel(Level.toLevel(System.getProperty("log.level"), Level.INFO));

        String configFile = args[args.length - 1];
        try (FileInputStream reader = new FileInputStream(configFile)) {
            Config config = new Yaml().loadAs(reader, Config.class);

            if (config.isOneShot()) {
                if (args.length != 1) {
                    System.err.println("For one-shot mode, only a single <yml configuration file> argument is allowed.");
                    System.exit(1);
                }
                runOneShot(config);
            } else {
                if (args.length != 3) {
                    System.err.println("For server mode, <hostname> <port> <yml configuration file> arguments are required.");
                    System.exit(1);
                }
                new WebServer().configure(config, args[0], Integer.parseInt(args[1]));
            }
        }
    }

    private static void runOneShot(Config config) throws Exception {
        String fsImageFilePath = config.getFsImageFile();
        if (null == fsImageFilePath || fsImageFilePath.isEmpty()) {
            LOG.error("oneShot mode requires fsImageFile to be set in configuration.");
            System.exit(1);
        }

        File fsImageFile = new File(fsImageFilePath);
        RandomAccessFile randomAccessFile = new RandomAccessFile(fsImageFile, "r");
        if (!fsImageFile.exists() || !fsImageFile.isFile()) {
            LOG.error("FSImage file does not exist or is not a file: {}", fsImageFilePath);
            System.exit(1);
        }

        LOG.info("One-shot mode: processing {}", fsImageFilePath);

        // Load FsImage
        LOG.info("Loading fsimage from {}", fsImageFilePath);
        long startTime = System.nanoTime();
        FsImageData fsImageData = new FsImageLoader.Builder().build().load(randomAccessFile);
        LOG.info("Finished loading fsimage in {}ms", (System.nanoTime() - startTime) / 1_000_000);

        // Compute stats
        LOG.info("Computing stats...");
        startTime = System.nanoTime();
        FsImageReporter.Report report = FsImageReporter.computeStatsReport(fsImageData, config);
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        LOG.info("Finished computing stats in {}ms.", durationMs);

        if (report.error) {
            LOG.error("Error during report computation.");
            System.exit(1);
        }

        // Collect and print metrics
        CollectorRegistry registry = new CollectorRegistry();

        // Register build info
        Info info = Info.build()
                .name("fsimage_exporter_build")
                .help("Hadoop FSImage exporter build info")
                .labelNames("appVersion", "buildTime", "buildScmVersion", "buildScmBranch")
                .register(registry);

        info.labels(
                BuildMetaInfo.INSTANCE.getVersion(),
                BuildMetaInfo.INSTANCE.getBuildTimeStamp(),
                BuildMetaInfo.INSTANCE.getBuildScmVersion(),
                BuildMetaInfo.INSTANCE.getBuildScmBranch()
        );

        // Add metadata metrics
        Gauge fsimage_last_run_success_time = Gauge.build()
                .name("fsimage_last_run_success_time")
                .help("Timestamp of last successful FSImage processing run.")
                .register(registry);

        fsimage_last_run_success_time.setToCurrentTime();

        Gauge fsimage_last_run_duration_seconds = Gauge.build()
                .name("fsimage_last_run_duration_seconds")
                .help("Duration of last FSImage processing run in seconds.")
                .register(registry);

        fsimage_last_run_duration_seconds.set(durationMs / 1000.0);

        // Register report metrics
        registry.register(new Collector() {
            @Override
            public List<Collector.MetricFamilySamples> collect() {
                List<Collector.MetricFamilySamples> mfs = new ArrayList<>();
                report.collect(mfs);
                return mfs;
            }
        });

        // Write to stdout
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(System.out))) {
            TextFormat.write004(writer, registry.metricFamilySamples());
        }
        LOG.info("Successfully wrote metrics to STDOUT.");
    }
}
