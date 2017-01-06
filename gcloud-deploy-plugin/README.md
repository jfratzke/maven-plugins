# Google Cloud Platform Deploy Plugin

## Summary

Many of our big data application follow a non-standard development and deploy process. This plugin was built to manage 
the configuration of where each necessary file needs to go as well as version the deployment on multiple machines

## Build

1. Clone the project
2. `mvn clean install`

## TODO

For now, the plugin works by pulling files out of the workspace rather than the artifact. The plan in the future is to
pull from the artifact.

## How To Use

* Bring this plugin into your project


    ```xml
    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>com.zulily.analytics</groupId>
                    <artifactId>gcloud-deploy-plugin</artifactId>
                    <version>${gcloud-deploy.version}</version>
                </plugin>
            </plugins>
        </pluginManagement>
    </build> 
    ```
    
* Configure the plugin such that you list the files you want to deploy


    ```xml
    <build>
        <plugins>
            <plugin>
                <groupId>com.zulily.analytics</groupId>
                <artifactId>gcloud-deploy-plugin</artifactId>
                <configuration>
                    <deployConfigs>
                        <deployConfig>
                            <gcloudProjectId>some-google-cloud-project</gcloudProjectId>
                            <gcloudServerId>some-google-cloud-server</gcloudServerId>
                            <zone>zone-of-the-server/zone>
                            <user>user-to-login-as</user>
                            <rootDir>root-directory to install applications</rootDir>
                            <files>
                                <file>${project.basedir}/src/main/resources/file1</file>
                                <file>${project.basedir}/src/main/resources/file2</file>
                                <file>${project.basedir}/src/main/resources/file3/file>
                            </files>
                            <versioned>true</versioned>
                        </deployConfig>
                    </deployConfigs>
                </configuration>
            </plugin>
        </plugins>
    </build>
    ```
    
* Deploy
    
    ```
    mvn your-project/pom.xml gcloud-deploy:gcloudDeploy
    ```
    
    
    
## Revert to a previous version
   
    mvn your-project/pom.xml gcloud-deploy:gcloudRevertToVersion -DprojectVersion=${version-to-revert-to} # e.g. 1.1.5