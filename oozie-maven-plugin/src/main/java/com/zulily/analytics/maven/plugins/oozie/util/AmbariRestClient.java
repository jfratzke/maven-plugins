package com.zulily.analytics.maven.plugins.oozie.util;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.core.util.Base64;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class AmbariRestClient {

    private static Logger log = LoggerFactory.getLogger(AmbariRestClient.class);

    private static final String REST_PREFIX = "/api/v1";
    private static final DefaultHttpClient client = new DefaultHttpClient();
    private byte[] AUTHORIZATION_STRING;
    private final HttpHost target;

    public AmbariRestClient(String server, String protocol, int port, String username, String password) {

        this.target = new HttpHost(server, port, Protocol.getProtocol(protocol));
        try {
            this.AUTHORIZATION_STRING = Base64.encode((username + ":" + password).getBytes("iso-8859-1"));
        } catch (UnsupportedEncodingException e) {
            log.error("AmbariRestClient creation failed => iso-8859-1 encoding is not supported");
            System.exit(0);
        }
    }

    /**
     * Fetches the names of the primary cluster
     * @return the name of hadoop cluster
     * @throws UnsupportedEncodingException
     */
    @SuppressWarnings("unchecked")
    public String getClusterName() {

        final String CLUSTER_PREFIX = "/clusters";
        HttpGet get = new HttpGet(this.target.toURI() + REST_PREFIX + CLUSTER_PREFIX);
        log.info("Fetching Cluster Name");

        HashMap<String, Object> result = this.executeRestCall(get);
        // This is so gross
        List<Map<String, Object>> items = (ArrayList<Map<String, Object>>) result.get("items");
        Map<String, String> clusters = (Map<String, String>) items.get(0).get("Clusters");
        return clusters.get("cluster_name");

    }

    @SuppressWarnings("unchecked")
    public String getNameNode() {

        final String NAMENODE_PREFIX = String.format("/clusters/%s/services/HDFS/components/NAMENODE", getClusterName());
        HttpGet get = new HttpGet(this.target.toURI() + REST_PREFIX + NAMENODE_PREFIX);
        log.info("Fetching NameNode Name");
        return extractHostName(this.executeRestCall(get));
    }


    public String getOozieServerName() {

        final String OOZIE_PREFIX = String.format("/clusters/%s/services/OOZIE/components/OOZIE_SERVER", getClusterName());
        HttpGet get = new HttpGet(this.target.toURI() + REST_PREFIX + OOZIE_PREFIX);
        log.info("Fetching Oozie Server Name");
        return extractHostName(this.executeRestCall(get));
    }

    /**
     * Return the resource manager address, will return the first valid address if cluster is running YARN HA
     * @return The name of a valid running resource manager on the hadoop cluster
     */
    public String getResourceManagerName() {

        Configuration config = getClientConfiguration("yarn-site");
        if (config.get("yarn.resourcemanager.ha.enabled").equalsIgnoreCase("true")) {
            log.info("Cluster is running YARN HA");
            for (String rmId : config.get("yarn.resourcemanager.ha.rm-ids").split(",")) {
                if (config.get("yarn.resourcemanager.address." + rmId) != null) {
                    return config.get("yarn.resourcemanager.address." + rmId);
                }
            }
        }
        return config.get("yarn.resourcemanager.address");
    }

    @SuppressWarnings("unchecked")
    private String extractHostName(HashMap<String, Object> jsonResult) {

        List<Map<String, Object>> components = (ArrayList<Map<String, Object>>) jsonResult.get("host_components");
        Map<String, String> roles = (Map<String, String>) components.get(0).get("HostRoles");
        return roles.get("host_name");
    }

    @SuppressWarnings("unchecked")
    public Configuration getClientConfiguration(String serviceName) {

        final Configuration serviceConfigurations = new Configuration();

        final String CONFIG_PREFIX = String.format("/clusters/%s/configurations?type=%s", getClusterName(), serviceName);
        HttpGet get = new HttpGet(this.target.toURI() + REST_PREFIX + CONFIG_PREFIX);
        log.info("Fetching Client Configuration for " + serviceName);

        HashMap<String, Object> result = executeRestCall(get);

        //Multiple versions of the configs typically exist, get the latest
        ArrayList<Map<String, Object>> versions = (ArrayList<Map<String, Object>>)result.get("items");
        int versionLength = versions.size();
        String latestVersionUri = "";
        for (Map<String, Object> version : versions) {
            if ((Integer)version.get("version") == versionLength)
                latestVersionUri = (String)version.get("href");
        }

        // Fetch Configurations
        log.info("Fetching configuration from URI: " + latestVersionUri);
        get = new HttpGet(latestVersionUri);
        result = executeRestCall(get);
        ArrayList<Map<String, Object>> items = (ArrayList<Map<String, Object>>)result.get("items");
        Map<String, String> properties = (Map<String, String>)items.get(0).get("properties");

        // Set hadoop configrations
        for (Object o : properties.entrySet()) {
            Map.Entry<String, String> pair = (Map.Entry) o;
            serviceConfigurations.set(pair.getKey(), pair.getValue());
        }
        return serviceConfigurations;
    }

    /**
     * Given a HttpGet object, execute the call and return the json response as a HashMap
     * @return the result of the call
     */
    @SuppressWarnings("unchecked")
    private HashMap<String, Object> executeRestCall(HttpGet request) {

        request.addHeader("Authorization", "Basic " + new String(AUTHORIZATION_STRING));
        try {
            HttpResponse response = client.execute(request);
            HttpEntity entity = response.getEntity();
            String jsonString = EntityUtils.toString(entity);
            return new ObjectMapper().readValue(jsonString, HashMap.class);
        } catch (IOException e) {
            log.error("Http Request failed: \n" + request.getURI() + "\n" + e.getMessage());
            System.exit(0);
        }
        return null;
    }

}
