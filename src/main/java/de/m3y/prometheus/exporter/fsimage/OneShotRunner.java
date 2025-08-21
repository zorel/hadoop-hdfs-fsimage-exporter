package de.m3y.prometheus.exporter.fsimage;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageLoader;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Info;
import io.prometheus.client.exporter.common.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a single FSImage report computation and prints metrics.
 */
public class OneShotRunner {
    private static final Logger LOG = LoggerFactory.getLogger(OneShotRunner.class);

    private OneShotRunner() {
        // Static utility class
    }

    public static void run(Config config) throws Exception {
        String fsImageFilePath = config.getFsImageFile();
        if (null == fsImageFilePath || fsImageFilePath.isEmpty()) {
            LOG.error("oneShot mode requires fsImageFile to be set in configuration.");
            System.exit(1);
        }

        File fsImageFile = new File(fsImageFilePath);
        if (!fsImageFile.exists() || !fsImageFile.isFile()) {
            LOG.error("FSImage file does not exist or is not a file: {}", fsImageFilePath);
            System.exit(1);
        }
        LOG.info("One-shot mode: processing {}", fsImageFilePath);

        // Load FsImage
        LOG.info("Loading fsimage from {}", fsImageFilePath);
        long startTime = System.nanoTime();
        final FsImageData fsImageData;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(fsImageFile, "r")) {
            fsImageData = new FsImageLoader.Builder().build().load(randomAccessFile);
        }
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
        Info.build()
                .name("fsimage_exporter_build")
                .help("Hadoop FSImage exporter build info")
                .labelNames("appVersion", "buildTime", "buildScmVersion", "buildScmBranch")
                .register(registry)
                .labels(
                        BuildMetaInfo.INSTANCE.getVersion(),
                        BuildMetaInfo.INSTANCE.getBuildTimeStamp(),
                        BuildMetaInfo.INSTANCE.getBuildScmVersion(),
                        BuildMetaInfo.INSTANCE.getBuildScmBranch()
                );

        // Add metadata metrics
        Gauge.build()
                .name("fsimage_last_run_success_time")
                .help("Timestamp of last successful FSImage processing run.")
                .register(registry)
                .setToCurrentTime();

        Gauge.build()
                .name("fsimage_last_run_duration_seconds")
                .help("Duration of last FSImage processing run in seconds.")
                .register(registry)
                .set(durationMs / 1000.0);

        // Register a single collector that mimics FsImageUpdateHandler
        final FsImageUpdateHandler.FsMetrics overall = new FsImageUpdateHandler.FsMetrics(
                FsImageCollector.METRIC_PREFIX);
        final FsImageUpdateHandler.FsMetrics userFsMetrics = new FsImageUpdateHandler.FsMetrics(FsImageUpdateHandler.METRIC_PREFIX_USER,
                new String[]{FsImageUpdateHandler.LABEL_USER_NAME});
        final FsImageUpdateHandler.FsMetrics groupFsMetrics = new FsImageUpdateHandler.FsMetrics(FsImageUpdateHandler.METRIC_PREFIX_GROUP,
                new String[]{FsImageUpdateHandler.LABEL_GROUP_NAME});
        final FsImageUpdateHandler.FsMetrics pathFsMetrics = new FsImageUpdateHandler.FsMetrics(FsImageUpdateHandler.METRIC_PREFIX_PATH,
                new String[]{FsImageUpdateHandler.LABEL_PATH});
        final FsImageUpdateHandler.FsMetrics pathSetFsMetrics = new FsImageUpdateHandler.FsMetrics(FsImageUpdateHandler.METRIC_PREFIX_PATH_SET,
                new String[]{FsImageUpdateHandler.LABEL_PATH_SET});

        // Populate the gauges
        overall.update(report.overallStats);
        for (FsImageReporter.UserStats userStat : report.userStats.values()) {
            userFsMetrics.update(userStat, userStat.userName);
        }
        for (FsImageReporter.GroupStats groupStat : report.groupStats.values()) {
            groupFsMetrics.update(groupStat, groupStat.groupName);
        }
        if (report.hasPathStats()) {
            for (FsImageReporter.PathStats pathStat : report.pathStats.values()) {
                pathFsMetrics.update(pathStat, pathStat.path);
            }
        }
        if (report.hasPathSetStats()) {
            for (FsImageReporter.PathStats pathStat : report.pathSetStats.values()) {
                pathSetFsMetrics.update(pathStat, pathStat.path);
            }
        }
        registry.register(new Collector() {
            @Override
            public List<Collector.MetricFamilySamples> collect() {
                List<Collector.MetricFamilySamples> mfs = new ArrayList<>();

                // Collect gauges
                overall.collect(mfs);
                userFsMetrics.collect(mfs);
                groupFsMetrics.collect(mfs);
                if (report.hasPathStats()) {
                    pathFsMetrics.collect(mfs);
                }
                if (report.hasPathSetStats()) {
                    pathSetFsMetrics.collect(mfs);
                }

                // Collect histograms and summaries from the report
                report.collect(mfs);

                return mfs;
            }
        });
        // registry.register(new FsImageCollector.ReportCollector(report, overall, userFsMetrics, groupFsMetrics, pathFsMetrics, pathSetFsMetrics));

        // Write to configured output file or to STDOUT
        final String outputFile = config.getOneShotOutputFile();
        final String destination = (outputFile != null && !outputFile.isEmpty()) ? outputFile : "STDOUT";

        try (Writer writer = (outputFile != null && !outputFile.isEmpty())
                ? new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))
                : new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {
            TextFormat.write004(writer, registry.metricFamilySamples());
        }
        LOG.info("Successfully wrote metrics to {}.", destination);
    }
}