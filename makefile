SHELL=/bin/sh

JAVAC = /usr/java/jdk1.8.0_162/bin/javac

build: wc.jar
	echo "Type 'make run_a5' to execute."

run_a5: wc.jar
	hdfs dfs -rm -r -f wordcountout  
	hadoop jar wc.jar wc.WordCount /user/rhodes/input wordcountout 
	#hadoop jar wc.jar wc.WordCount /user/rhodes/ME1879 wordcountout 
	hdfs dfs -cat wordcountout/part* | more

# To run the complete set of jobs necessary to find the most
# frequently occurring letters, make sure the line ABOVE this
# comment is commented out, and then run "make run_all"

run_sort: run_a5
	hdfs dfs -rm -r -f  sortbycountout
	hadoop jar wc.jar wc.SortByCount wordcountout sortbycountout
	#hdfs dfs -cat sortbycountout/part* | more

run_all: run_a5 run_sort
	hdfs dfs -rm -r -f  aggregateout
	hadoop jar wc.jar wc.Aggregate sortbycountout aggregateout
	hdfs dfs -cat aggregateout/part* | more



	
wc.jar: wc  wc/WordCount.class  wc/SortByCount.class  wc/Aggregate.class
	jar cvf wc.jar wc

wc/WordCount.class: wc WordCount.java 
	cp WordCount.java wc
	$(JAVAC) wc/WordCount.java -cp `hadoop classpath`

wc/SortByCount.class: wc SortByCount.java
	cp SortByCount.java wc
	$(JAVAC) wc/SortByCount.java -cp `hadoop classpath`

wc/Aggregate.class: wc Aggregate.java
	cp Aggregate.java wc
	$(JAVAC) wc/Aggregate.java -cp `hadoop classpath`

wc:
	mkdir -p wc	

clean: 
	rm -rf wc.jar wc	
