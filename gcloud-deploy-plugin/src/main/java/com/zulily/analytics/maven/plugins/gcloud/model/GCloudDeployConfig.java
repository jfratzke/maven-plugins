package com.zulily.analytics.maven.plugins.gcloud.model;


import org.apache.maven.plugins.annotations.Parameter;

import java.util.List;

public class GCloudDeployConfig {

    @Parameter(property = "gcloudProjectId")
    private String gcloudProjectId;

    @Parameter(property = "gcloudServerId")
    private String gcloudServerId;

    @Parameter(property = "zone")
    private String zone;

    @Parameter(property = "user")
    private String user;

    @Parameter(property = "rootDir")
    private String rootDir;

    @Parameter(property = "files")
    private List<String> files;

    @Parameter(property = "versioned", defaultValue = "false")
    private boolean versioned;

    public String getGcloudProjectId() {
        return gcloudProjectId;
    }

    public String getGcloudServerId() {
        return gcloudServerId;
    }

    public String getZone() {
        return zone;
    }

    public String getUser() {
        return user;
    }

    public String getRootDir() {
        return rootDir;
    }

    public List<String> getFiles() {
        return files;
    }

    public boolean isVersioned() {
        return versioned;
    }
}
