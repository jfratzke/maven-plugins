package com.zulily.analytics.maven.plugins.oozie;


import com.zulily.analytics.maven.plugins.oozie.model.CoordinatorConfig;
import com.zulily.analytics.maven.plugins.oozie.util.AmbariRestClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.*;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import static org.apache.oozie.client.Job.Status.*;

public class HadoopCluster {

    private final AmbariRestClient ambari;
    private static Logger log = LoggerFactory.getLogger(HadoopCluster.class);
    private static OozieClient oozieClient;

    public HadoopCluster(AmbariRestClient arc) {
        this.ambari = arc;
    }

    public void killCoordinator(String coordinatorName, String username) {
        try {
            OozieClient client = getOozieClient();

            for (CoordinatorJob coordinatorJob : client.getCoordJobsInfo("name=" + coordinatorName + ";user=" + username, 1, 1000)) {
                if (runnableStates.contains(coordinatorJob.getStatus())) {
                    log.info("Killing coordinator: " + coordinatorName + "(" + coordinatorJob.getId() + ") from user: " + username);
                    oozieClient.kill(coordinatorJob.getId());
                }
            }
        } catch (OozieClientException e) {
            throw new RuntimeException(e);
        }
    }

    public void SubmitCoordinator(CoordinatorConfig coordinator, String projectDir, File jarfile, Properties oozieProperties,
                                  Integer frequencyMinutes, Integer offset, List<File> applicationFiles) {

        String namenode = ambari.getNameNode();
        log.info("Submitting coordinator");
        // Populate oozieProperties with the common set of properties needed by each coordinator
        oozieProperties.put("nameNode", "hdfs://" + namenode + ":8020");
        oozieProperties.put("jobTracker", ambari.getResourceManagerName());
        oozieProperties.put("yarn.resourcemanager.address", ambari.getResourceManagerName());
        oozieProperties.put("oozie.use.system.libpath", "true");

        String coordinatorName = coordinator.getName();

        FileSystem hdfs = null;
        try {
            hdfs = getHdfs(oozieProperties.getProperty(OozieClient.USER_NAME));
            log.info("User home directory " + hdfs.getHomeDirectory());
            Path destApplicationDir = getApplicationPath(hdfs, coordinatorName, projectDir);
            log.info("destApplicationDir: " + destApplicationDir);

            // Make a clean application dir
            makeCleanDir(hdfs, destApplicationDir);

            // Copy the jarfile to the application dir
            if (jarfile.exists()) {
                copyFileToHdfs(hdfs, new Path(jarfile.toString()), new Path(destApplicationDir, "lib/" + jarfile.getName()));
                oozieProperties.put("appJar", destApplicationDir + "/lib/" + jarfile.getName());
            }

            // Copy application resources to the application dir
            copyApplicationFilesToHdfs(hdfs, applicationFiles, destApplicationDir);

            // Create and deploy job properties file to application dir
            deployPropertiesFile(hdfs, oozieProperties, new Path(destApplicationDir, "job.properties"));

            // Kill existing coordinator if exists
            log.info("User is " + oozieProperties.getProperty("user.name"));
            killCoordinator(coordinatorName, oozieProperties.getProperty("user.name"));

            // Create and deploy coordinator properties file to application dir
            Properties currentCoordProperties = getCoordinatorProperties(coordinator, oozieProperties,
                    destApplicationDir.toString(), coordinatorName, destApplicationDir, coordinator.getStartTime(),
                    coordinator.getEndTime(), frequencyMinutes, offset);
            log.info("StartTime is " + currentCoordProperties.get("start"));
            log.info("EndTime is " + currentCoordProperties.get("end"));

            // Submit the coordinator
            String jobId = getOozieClient().submit(currentCoordProperties);
            log.info("Started Oozie coordinator: " + coordinatorName + "(" + jobId + ")");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (OozieClientException e) {
            throw new RuntimeException(e);
        } finally {
            if (hdfs != null) {
                try {
                    hdfs.close();
                } catch (IOException e) {
                }
            }
        }


    }

    /**
     * @param hdfs
     * @param projectName e.g. zu-bidw-spark-1.0.1
     * @return the destination (hdfs) application directory for a coordinator or workflow.
     * @throws IOException
     * @throws InterruptedException
     */
    public Path getApplicationPath(FileSystem hdfs, String coordinatorName, String projectName) throws IOException, InterruptedException{
        return new Path(hdfs.getHomeDirectory(), "workflows/" + coordinatorName + "/" + projectName);
    }

    private Properties getCoordinatorProperties(CoordinatorConfig coordinator, Properties oozieProperties,
                                                String applicationPath, String coordName, Path coordPath,
                                                DateTime startTime, DateTime endTime, int frequencyMinutes, int offset) {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Properties coordProperties = new Properties();
        coordProperties.putAll(oozieProperties);
        coordProperties.put("coordName", coordName);
        coordProperties.put("appPath", applicationPath);
        coordProperties.put("oozie.coord.application.path", applicationPath + "/" + coordinator.getXmlFilename());
        coordProperties.put("start", dateFormat.format(startTime.toDate()));
        coordProperties.put("end", dateFormat.format(endTime.toDate()));
        coordProperties.put("initialDataset", dateFormat.format(startTime.minusMinutes(frequencyMinutes * offset).toDate()));
        coordProperties.putAll(coordinator.getProperties());
        return coordProperties;
    }

    protected FileSystem getHdfs(String username) throws IOException, InterruptedException {
        try {
            Configuration config = ambari.getClientConfiguration("hdfs-site");
            config.addResource(ambari.getClientConfiguration("core-site"));
            config.set("dfs.client.use.datanode.hostname","true");
            return FileSystem.get(new URI("hdfs://" + ambari.getNameNode() + ":8020"), config, username);
        } catch (URISyntaxException e) {
            log.error("Invalid Namenode URI configuration");
            System.exit(0);
        }
        return null;
    }

    private OozieClient getOozieClient() {

        if (oozieClient == null)
            oozieClient = new OozieClient("http://" + ambari.getOozieServerName() + ":11000/oozie");
        return oozieClient;
    }

    /**
     * If dir exists in HDFS, delete it.  Then make dir in HDFS and all its missing parent dirs.
     */
    private void makeCleanDir(FileSystem hdfs, Path dir) throws IOException, InterruptedException {
        if (hdfs.exists(dir)) {
            hdfs.delete(dir, true);
        }
        hdfs.mkdirs(dir);
    }

    /**
     * Keep a record of the properties submitted to oozie for this job on HDFS
     * @param hdfs
     * @param properties
     * @param filePath
     * @throws IOException
     * @throws InterruptedException
     */
    private void deployPropertiesFile(FileSystem hdfs, Properties properties, Path filePath) throws IOException, InterruptedException {
        log.info("Generating properties file: " + filePath.toString());
        FSDataOutputStream jobPropertiesFile = hdfs.create(filePath);
        try {
            properties.store(jobPropertiesFile, "Autogenerated by submitCoordinator");
        } finally {
            jobPropertiesFile.close();
        }
    }

    /**
     * Copy each File object in applicationFiles to a corresponding file within the destApplicationDir on HDFS.
     */
    private void copyApplicationFilesToHdfs(FileSystem hdfs, List<File> applicationFiles, Path destApplicationDir) throws IOException {
        // Copy application files to the application path
        for (File af : applicationFiles) {
            log.info("Copying application file to " + destApplicationDir);
            hdfs.copyFromLocalFile(new Path(af.getPath()), destApplicationDir);
        }
    }

    /**
     * Copy a file or directory from the local filesystem to HDFS.
     */
    private void copyFileToHdfs(FileSystem hdfs, Path localFilePath, Path hdfsFilePath) throws IOException, InterruptedException {
        log.info("Copying " + localFilePath + " to " + hdfsFilePath);
        hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
    }

    private static EnumSet<Job.Status> runnableStates = EnumSet.of(PAUSED, PAUSEDWITHERROR, PREMATER, PREP, PREPPAUSED,
            PREPSUSPENDED, RUNNING, RUNNINGWITHERROR, SUSPENDED, SUSPENDEDWITHERROR);
}
