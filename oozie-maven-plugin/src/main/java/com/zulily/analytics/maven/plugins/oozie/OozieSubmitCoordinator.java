package com.zulily.analytics.maven.plugins.oozie;


import com.zulily.analytics.maven.plugins.oozie.model.CoordinatorConfig;
import com.zulily.analytics.maven.plugins.oozie.util.AmbariRestClient;
import com.zulily.analytics.maven.plugins.oozie.util.PluginUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

@Mojo(name = "submitCoordinator")
public class OozieSubmitCoordinator extends AbstractMojo {

    private static Logger log = LoggerFactory.getLogger(OozieSubmitCoordinator.class);

    @Parameter(property = "coordinatorName")
    protected String coordinatorName;

    @Parameter(property = "applicationName", defaultValue = "${project.build.finalName}")
    protected String applicationName; // e.g. test-1.0.4-SNAPSHOT

    @Parameter(property = "jarfile", defaultValue = "${project.build.directory}/${project.build.finalName}.jar")
    protected File jarfile;

    @Parameter(property = "coordinatorFile", defaultValue = "hourly.coord.xml")
    protected String coordinatorFile;

    @Parameter(property = "startDate")
    protected String startDate;

    @Parameter(property = "startHour")
    protected String startHour;

    @Parameter(property = "username")
    protected String username;

    @Parameter(property = "usergroup")
    protected String usergroup;

    @Parameter(property = "frequencyMinutes", defaultValue = "60")
    protected int frequencyMinutes;

    @Parameter(property = "offset", defaultValue = "0")
    protected int offset;

    @Parameter(property = "duration", defaultValue = "P2Y")
    protected String duration;

    @Parameter(property = "alertEmail", defaultValue = "jfratzke@zulily.com")
    protected String alertEmail;

    @Parameter(property = "resourceDir", defaultValue = "${project.basedir}/src/main/resources/${project.artifactId}")
    protected String resourceDir;

    @Parameter(property = "ambariHost")
    protected String ambariHost;

    @Parameter(property = "ambariPort", defaultValue = "8080")
    protected int ambariPort;

    @Parameter(property = "ambariProtocol", defaultValue = "http")
    protected String ambariProtocol;

    @Parameter(property = "ambariUser")
    protected String ambariUser;

    @Parameter(property = "ambariPW")
    protected String ambariPW;

    public void execute() throws MojoExecutionException, MojoFailureException {

        Properties oozieProperties = new Properties();
        oozieProperties.put("frequencyMinutes", String.valueOf(frequencyMinutes));
        oozieProperties.put("user.name", username);
        oozieProperties.put("group.name", usergroup);
        oozieProperties.put("duration", duration);
        oozieProperties.put("alertEmail", alertEmail);

        File resourcesDir = new File(resourceDir);


        CoordinatorConfig config = new CoordinatorConfig(coordinatorName,applicationName,coordinatorFile, startDate, startHour, duration, null);
        AmbariRestClient arc = new AmbariRestClient(ambariHost, ambariProtocol, ambariPort, ambariUser, ambariPW);
        HadoopCluster hc = new HadoopCluster(arc);
        DateTime startDateTime = PluginUtils.parseDateTime(startDate, startHour);
        DateTime endDateTime = PluginUtils.addDuration(startDateTime, duration);
        log.info("Fetching Resources from " + resourceDir);
        hc.SubmitCoordinator(config, applicationName, jarfile, oozieProperties, frequencyMinutes, offset, Arrays.asList(resourcesDir.listFiles()));

    }
}
