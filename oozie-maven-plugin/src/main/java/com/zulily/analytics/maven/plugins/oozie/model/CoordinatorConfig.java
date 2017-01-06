package com.zulily.analytics.maven.plugins.oozie.model;

import com.zulily.analytics.maven.plugins.oozie.util.PluginUtils;
import org.joda.time.DateTime;
import java.util.Properties;

/**
 * This class roughly mirrors the configuration originally contained in the Maven Ddc Oozie Coordinator Application class.
 */
public class CoordinatorConfig {
    private final String name;
    private final String applicationName; // the name of the application resources directory, e.g. web-ana-pixall-scalding
    private final String xmlFilename; // the basename of the coordinator XML file located in the application dir.  e.g. hourly.coord.xml.
    private final DateTime startTime;
    private final DateTime endTime;
    private final Properties properties;

    /**
     * Required config: name, path
     * Optional config: startHour, startDate, properties.  Just leave them null.
     */
    public CoordinatorConfig(String name, String applicationName, String xmlFilename,
                             String startDate, String startHour, String duration, Properties properties) {
        this.name = name;
        this.applicationName = applicationName;
        this.xmlFilename = xmlFilename;
        this.startTime = PluginUtils.parseDateTime(startDate, startHour);
        this.endTime = PluginUtils.addDuration(this.startTime, duration);
        this.properties = (properties == null ? new Properties() : properties);
        check();
    }

    @Override
    public String toString() {
        return "CoordinatorConfig{" +
                "name='" + name + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", xmlFilename='" + xmlFilename + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", properties=" + properties +
                '}';
    }

    public String getName() {
        return name;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getXmlFilename() {
        return xmlFilename;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    public Properties getProperties() {
        return properties;
    }

    private void check() {
        if (this.name.isEmpty()) {
            throw new IllegalArgumentException("Coordinator name cannot be empty");
        }
        if (this.applicationName.isEmpty()) {
            throw new IllegalArgumentException("Coordinator application name cannot be empty");
        }
        if (this.xmlFilename.isEmpty()) {
            throw new IllegalArgumentException("Coordinator xml file name cannot be empty");
        }
    }
}

