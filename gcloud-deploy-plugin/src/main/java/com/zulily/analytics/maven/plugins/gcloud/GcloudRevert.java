package com.zulily.analytics.maven.plugins.gcloud;


import com.zulily.analytics.maven.plugins.gcloud.model.GCloudDeployConfig;
import com.zulily.analytics.maven.plugins.gcloud.utils.GcloudFileSystem;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Mojo(name = "gcloudRevertToVersion")
public class GcloudRevert extends AbstractMojo {

    private static Logger log = LoggerFactory.getLogger(GcloudRevert.class);

    @Parameter(property = "deployConfigs")
    private List<GCloudDeployConfig> deployConfigs;

    @Parameter(property = "projectId", defaultValue = "${project.name}")
    private String projectId;

    @Parameter(property = "projectVersion", defaultValue = "${project.version}")
    private String projectVersion;

    public void execute() throws MojoExecutionException, MojoFailureException {
        for (GCloudDeployConfig gdc : deployConfigs) {
            if (gdc.isVersioned())
                revert(gdc);
        }
    }

    private void revert(GCloudDeployConfig gdc) throws MojoExecutionException {
        GcloudFileSystem gfs = new GcloudFileSystem(gdc.getGcloudProjectId(),gdc.getZone(), gdc.getGcloudServerId(), gdc.getUser());
        String targetDir = gdc.getRootDir() + "/" + projectId + "/" + projectVersion;
        String linkName = gdc.getRootDir() + "/" + projectId + "/current";
        if (!gfs.createSymbolicLink(linkName, targetDir))
            throw new MojoExecutionException("Revert of versioned application failed.");
    }
}
