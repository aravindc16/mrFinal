
DIR := $(shell basename `pwd`)

$(DIR).jar: *.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR)-all.jar $(DIR).jar

run: $(DIR).jar clean-output
	hadoop jar $(DIR).jar data.tsv.bz2

clean:
	rm -rf build bin *.jar .gradle test.log


