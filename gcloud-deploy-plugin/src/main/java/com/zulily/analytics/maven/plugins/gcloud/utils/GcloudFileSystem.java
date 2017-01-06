package com.zulily.analytics.maven.plugins.gcloud.utils;


import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * Creates a GcloudFileSystem enable access to perform generic filesystem commands on the specified system
 */
public class GcloudFileSystem extends GcloudService {

    private String server;
    private String zone;
    private String user;
    private String[] baseGcloudBuilder;

    /**
     * @param project Name of the google cloud project
     * @param zone Zone of the specified compute instance (e.g. us-central1-b)
     * @param server Hostname given to the specific compute instance
     * @param user User name to login as
     */
    public GcloudFileSystem(String project, String zone, String server, String user) {
        super(project);
        this.server = server;
        this.zone = zone;
        this.user = user;
        this.baseGcloudBuilder = new String[] {"gcloud", "compute", "--project", project};
    }

    public boolean createSymbolicLink(String linkName, String target) {
        return executeSSH(new String[] {"--command", "ln -sfn " + target + " " + linkName});
    }

    public boolean mkdir(String dir) {
        return executeSSH(new String[] {"--command", "mkdir -p " + dir});
    }

    public boolean rm(String dir) {
        return executeSSH(new String[] {"--command", "rm -r " + dir});
    }

    public boolean copyFiles(List<String> filePaths, String target) {
        for (String path : filePaths)
            if (!copyFile(path, target))
                return false;
        return true;
    }

    public boolean copyFile(String src, String target) {
        return executeCopyFiles(new String[] {src, this.user + "@" + this.server + ":" + target});
    }

    private boolean executeSSH(String[] args) {
        return execute(arrayConcat(arrayConcat(this.baseGcloudBuilder, new String[]{"ssh", "--zone", this.zone}), ArrayUtils.add(args, this.user + "@" + this.server)));
    }

    private boolean executeCopyFiles(String[] args) {
        return execute(arrayConcat(this.baseGcloudBuilder, arrayConcat(new String[]{"copy-files", "--zone", this.zone}, args)));
    }
}
