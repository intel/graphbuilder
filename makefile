all:
	mvn clean package
	cd target; ~/graphbuilder/scripts/mpirsync

test:
	mvn clean test

clean:
	mvn clean
