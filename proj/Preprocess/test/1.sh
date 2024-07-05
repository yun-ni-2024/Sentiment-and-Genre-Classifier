#!/bin/bash

LIB_DIR="lib"
POM_DEPENDENCIES=""

for jar in $(ls $LIB_DIR/*.jar); do
    JAR_NAME=$(basename $jar)
    POM_PROPS=$(unzip -p $jar META-INF/maven/*/*/pom.properties)
    
    GROUP_ID=$(echo "$POM_PROPS" | grep '^groupId=' | cut -d'=' -f2)
    ARTIFACT_ID=$(echo "$POM_PROPS" | grep '^artifactId=' | cut -d'=' -f2)
    VERSION=$(echo "$POM_PROPS" | grep '^version=' | cut -d'=' -f2)

    if [ -n "$GROUP_ID" ] && [ -n "$ARTIFACT_ID" ] && [ -n "$VERSION" ]; then
        POM_DEPENDENCIES="$POM_DEPENDENCIES
    <dependency>
        <groupId>$GROUP_ID</groupId>
        <artifactId>$ARTIFACT_ID</artifactId>
        <version>$VERSION</version>
        <scope>system</scope>
        <systemPath>\${basedir}/$LIB_DIR/$JAR_NAME</systemPath>
    </dependency>"
    fi
done

echo "<dependencies>$POM_DEPENDENCIES
</dependencies>"

