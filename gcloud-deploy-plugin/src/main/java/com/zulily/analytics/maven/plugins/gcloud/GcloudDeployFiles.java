package com.zulily.analytics.maven.plugins.gcloud;

import com.zulily.analytics.maven.plugins.gcloud.model.GCloudDeployConfig;
import com.zulily.analytics.maven.plugins.gcloud.utils.GcloudFileSystem;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.util.List;

@Mojo(name = "gcloudDeploy")
public class GcloudDeployFiles extends AbstractMojo {

    private static Logger log = LoggerFactory.getLogger(GcloudDeployFiles.class);

    @Parameter(property = "deployConfigs")
    private List<GCloudDeployConfig> deployConfigs;

    @Parameter(property = "projectId", defaultValue = "${project.name}")
    private String projectId;

    @Parameter(property = "projectVersion", defaultValue = "${project.version}")
    private String projectVersion;

    public void execute() throws MojoExecutionException, MojoFailureException {

        for (GCloudDeployConfig gdc : deployConfigs) {
            if (gdc.isVersioned())
                deployVersionedApp(gdc);
            else
                deployNonVersionedApp(gdc);
        }
    }

    private void deployVersionedApp(GCloudDeployConfig gdc) throws MojoExecutionException, MojoFailureException {
        GcloudFileSystem gfs = new GcloudFileSystem(gdc.getGcloudProjectId(),gdc.getZone(), gdc.getGcloudServerId(), gdc.getUser());
        gfs.mkdir(gdc.getRootDir() + "/" + projectId);

        String targetDir = gdc.getRootDir() + "/" + projectId + "/" + projectVersion;

        gfs.rm(gdc.getRootDir() + "/" + projectId + "/" + projectVersion);
        if (!gfs.mkdir(targetDir) ||
                !gfs.createSymbolicLink(gdc.getRootDir() + "/" + projectId + "/current", targetDir))
            throw new MojoFailureException("Failed to created needed directory structure");
        for (String s : gdc.getFiles()) {
            if (!gfs.copyFile(s, targetDir))
                throw new MojoFailureException("Failed to copy file " + s + " to target gcloud directory");
        }
    }

    private void deployNonVersionedApp(GCloudDeployConfig gdc) throws MojoExecutionException, MojoFailureException {
        GcloudFileSystem gfs = new GcloudFileSystem(gdc.getGcloudProjectId(),gdc.getZone(), gdc.getGcloudServerId(), gdc.getUser());
        gfs.mkdir(gdc.getRootDir() + "/" + projectId);

        String targetDir = gdc.getRootDir() + "/" + projectId;

        gfs.rm(gdc.getRootDir() + "/" + projectId);
        if (!gfs.mkdir(targetDir))
            throw new MojoFailureException("Failed to created needed directory structure");
        for (String s : gdc.getFiles()) {
            if (!gfs.copyFile(s, targetDir))
                throw new MojoFailureException("Failed to copy file " + s + " to target gcloud directory");
        }
    }
}
