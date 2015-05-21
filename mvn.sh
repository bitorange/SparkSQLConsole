mvn install:install-file -Dfile=lib/j-text-utils.jar -DgroupId=dnl -DartifactId=j-text-utils -Dversion=0.3.3 -Dpackaging=jar
mvn clean install -Dmaven.compiler.target=1.7 -Dmaven.compiler.source=1.7 -DskipTests=true -B -X
