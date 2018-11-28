# set permissions via: chmod u=rwx importDeequFatJar.sh

cd ../deequ
mvn assembly:assembly -DdescriptorId=jar-with-dependencies -DskipTests
cp target/deequ-1.0.0-SNAPSHOT-jar-with-dependencies.jar ../backend/lib
