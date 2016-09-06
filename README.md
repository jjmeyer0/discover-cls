# Prerequisites: Make Sure the Following are Installed

* [Maven](https://maven.apache.org/download.cgi)
* Java 1.8
 
# Installation Instructions

1. Change directories to discover-cls
2. Run the following commands to package NiFi processors

```bash
mvn clean package
```

3. Move custom nar to NiFi lib directory. The lib directories are configured in nifi.properties. By default there is nifi.nar.library.directory property. 
By default there is a lib directory where NiFi is installed.

```bash
cp nifi-discover-cls-nar/target/nifi-discover-cls-nar-*.nar <nifi-lib>
```

4. Restart NiFi to register new processors

```bash
<nifi-installation>/bin/nifi.sh restart
```

# Project Layout

## nifi-discover-cls-nar

This module will package the custom processors and creates the artificat that will be deployed to NiFi in order to use custom processors.

## nifi-discover-cls-processors

This contains the Java code and JUnit tests for the custom processors.

## processor-templates

This directory contains example flows of the custom processors defined in this project.