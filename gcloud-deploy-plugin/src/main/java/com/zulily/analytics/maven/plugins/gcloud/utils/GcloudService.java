package com.zulily.analytics.maven.plugins.gcloud.utils;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;

public abstract class GcloudService {

    private Logger log = LoggerFactory.getLogger(GcloudService.class);
    protected String projectId;

    public GcloudService(String projectId) {
        this.projectId = projectId;
    }

    protected boolean execute(String[] arguments) {
        log.info("Executing {}", StringUtils.join(arguments, ' '));
        try {
            Process proc = Runtime.getRuntime().exec(arguments);
            if (proc.waitFor() == 0) {
                log.info("Successful");
                return true;
            } else {
                String line;
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
                while ((line = in.readLine()) != null) {
                    log.warn(line);
                }
                return false;
            }
        } catch (IOException ioe) {
            log.error(stackTrace(ioe));
        } catch (InterruptedException ie) {
            log.error(stackTrace(ie));
        }
        return false;
    }

    private String stackTrace(Exception cause) {
        if (cause == null)
            return "";
        StringWriter sw = new StringWriter(1024);
        final PrintWriter pw = new PrintWriter(sw);
        cause.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    public static <T> T[] arrayConcat(T[] first, T[] second) {
        T[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
