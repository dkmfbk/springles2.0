# Springles2.0

SParql-based Rule Inference over Named Graphs Layer Extending Sesame)

# Developers Guide

**SPRINGLES** is developed using **MAVEN** (http://maven.apache.org). You need a basic understanding of MAVEN in order to compile the code and generate the documentation.

# MAVEN instructions


In the following we gave a brief overview of the main commands to work with the source package. You need to install MAVEN version 3 or greater on your machine (Windows, Linux, Mac OSX) in order to follow these instructions.
Installing the package and prepare the built environment

Unzip the package where you prefer on your file system. Open a command prompt and enter the following command:
```sh
    mvn -v
```
This should display the version of MAVEN currently installed. Please check you have version 3 of Maven installed, otherwise please update your installation.

It would be also helpful to allocate more memory to MAVEN for its execution: to this end, define the following environment variable (in Windows: select control panel | system):
```sh
  MAVEN_OPTS = -Xms128m -Xmx256m
```
Now you should be ready to compile the code.
Compile the code

From the command line, move to the root directory of the source package and execute the following command:
```sh
mvn package -Dmaven.test.skip
```
This tells MAVEN to compile the code and generate the resulting JARs; switch "-Dmaven.test.skip" causes MAVEN to skip JUnit tests (you may omit it if you want to perform testing before packaging). If everything works you should see a "BUILD SUCCESSFUL" message at the end.
Generate the documentation

Launch the following two commands from the prompt:

```sh
mvn site -Dmaven.test.skip
mvn site:deploy
```

The first command generates the HTML documentation from the APT sources (a MAVEN format, used to write these pages) and generates also all the configured MAVEN reports (including Javadocs). The second command deploys the generated documentation under the /docs folder under the package (you can change the pom.xml to deploy to a different location / Web site).

In order to generate the PDF manual, the Doxia MAVEN plugin has been configured to translate part of the documentation into Latex. You need to create the site, then manually enter folder /target/doxia/latex/manual, open manual.tex, change the document type from book to article (remove also the /chapter instruction) and then compile the document using your preferred Latex distribution (e.g., MikTeX on Windows).
Generate the source and binary packages

From the command prompt, execute the following:
```sh
mvn assembly:assembly -Dmaven.test.skip
```

MAVEN will generate two zip files for the source and the binary packages, under the /target folder. These files are exactly the packages published on the web site. Please update the assembly.xml descriptors under /src/main/assemblies subfolders if you wish to customize the generated packages.
Note on local MAVEN repository

This package includes a local MAVEN repository (/mvnrepo)containing JAR files for external libraries not available on public MAVEN repositories at the time SPRINGLES was built.
