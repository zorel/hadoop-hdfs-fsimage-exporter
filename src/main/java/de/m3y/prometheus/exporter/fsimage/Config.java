package de.m3y.prometheus.exporter.fsimage;

import java.util.*;

import de.m3y.hadoop.hdfs.hfsa.util.IECBinary;

/**
 * Config options for collector.
 */
public class Config {
    /**
     * Default file size distribution bucket limits.
     */
    private static final List<String> DEFAULT_FILE_SIZE_DISTRIBUTION_BUCKETS =
            Collections.unmodifiableList(Arrays.asList(
                    "0", "1 MiB", "32 MiB", "64 MiB", "128 MiB", "1 GiB", "10 GiB"
            ));

    /**
     * Path where HDFS NameNode stores fsimage file snapshots
     */
    private String fsImagePath;
    /**
     * A list of paths to report statistics for.
     * <p>
     * Paths can contain a regexp postfix, like "/users/ab.*", for matching direct child directories
     */
    private Set<String> paths;
    /**
     * Path sets are grouped paths by an identifier.
     *
     * @see #paths
     */
    private Map<String, List<String>> pathSets;

    /**
     * A regex pattern for user names to report statistics for.
     * If not set, statistics for all users are reported.
     */
    private String userNameRegex;

    /**
     * If set, run in one-shot mode processing a single fsimage file and printing metrics to STDOUT.
     */
    private boolean oneShot = false;

    /**
     * The fsimage file to process in one-shot mode.
     */
    private String fsImageFile;

    /**
     * Skip file size distribution for group stats.
     */
    private boolean skipFileDistributionForGroupStats = false;
    /**
     * Skip file size distribution for user stats.
     */
    private boolean skipFileDistributionForUserStats = false;
    /**
     * Skip file size distribution for path based stats.
     */
    private boolean skipFileDistributionForPathStats = false;
    /**
     * Skip file size distribution for path set based stats.
     */
    private boolean skipFileDistributionForPathSetStats = false;
    /**
     * File size distribution buckets, supporting IEC units of KiB, MiB, GiB, TiB, PiB
     */
    private List<String> fileSizeDistributionBuckets = DEFAULT_FILE_SIZE_DISTRIBUTION_BUCKETS;

    public String getFsImagePath() {
        return fsImagePath;
    }

    public void setFsImagePath(String fsImagePath) {
        this.fsImagePath = fsImagePath;
    }

    public Set<String> getPaths() {
        return paths;
    }

    public void setPaths(Set<String> paths) {
        this.paths = paths;
    }

    public boolean hasPaths() {
        return null != paths && !paths.isEmpty();
    }

    public boolean isSkipFileDistributionForPathStats() {
        return skipFileDistributionForPathStats;
    }

    public void setSkipFileDistributionForPathStats(boolean skipFileDistributionForPathStats) {
        this.skipFileDistributionForPathStats = skipFileDistributionForPathStats;
    }

    public boolean isSkipFileDistributionForGroupStats() {
        return skipFileDistributionForGroupStats;
    }

    public void setSkipFileDistributionForGroupStats(boolean skipFileDistributionForGroupStats) {
        this.skipFileDistributionForGroupStats = skipFileDistributionForGroupStats;
    }

    public boolean isSkipFileDistributionForUserStats() {
        return skipFileDistributionForUserStats;
    }

    public void setSkipFileDistributionForUserStats(boolean skipFileDistributionForUserStats) {
        this.skipFileDistributionForUserStats = skipFileDistributionForUserStats;
    }

    public Map<String, List<String>> getPathSets() {
        return pathSets;
    }

    public void setPathSets(Map<String, List<String>> pathSets) {
        this.pathSets = pathSets;
    }

    public String getUserNameRegex() {
        return userNameRegex;
    }

    public void setUserNameRegex(String userNameRegex) {
        this.userNameRegex = userNameRegex;
    }

    public boolean hasUserNameRegex() {
        return userNameRegex != null && !userNameRegex.isEmpty();
    }

    public boolean isOneShot() {
        return oneShot;
    }

    public void setOneShot(boolean oneShot) {
        this.oneShot = oneShot;
    }

    private String oneShotOutputFile;

    public String getOneShotOutputFile() {
        return oneShotOutputFile;
    }

    public void setOneShotOutputFile(String oneShotOutputFile) {
        this.oneShotOutputFile = oneShotOutputFile;
    }

    public String getFsImageFile() {
        return fsImageFile;
    }

    public void setFsImageFile(String fsImageFile) {
        this.fsImageFile = fsImageFile;
    }

    public boolean hasPathSets() {
        return pathSets != null && !pathSets.isEmpty();
    }

    public boolean isSkipFileDistributionForPathSetStats() {
        return skipFileDistributionForPathSetStats;
    }

    public void setSkipFileDistributionForPathSetStats(boolean skipFileDistributionForPathSetStats) {
        this.skipFileDistributionForPathSetStats = skipFileDistributionForPathSetStats;
    }

    public List<String> getFileSizeDistributionBuckets() {
        return fileSizeDistributionBuckets;
    }

    public void setFileSizeDistributionBuckets(List<String> fileSizeDistributionBuckets) {
        this.fileSizeDistributionBuckets = fileSizeDistributionBuckets;
    }

    public double[] getFileSizeDistributionBucketsAsDoubles() {
        return getFileSizeDistributionBuckets().stream().mapToDouble(IECBinary::parse).toArray();
    }
}
