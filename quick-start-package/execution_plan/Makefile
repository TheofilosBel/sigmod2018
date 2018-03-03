CC = g++
CFLAGS = -Wall -std=c++11
LIBS = -lm
EXEC = exec_plan
OBJECTS = main.o functions.o
SOURCES = src/main.cpp src/functions.cpp
HEADERS = include/header.hpp include/functions.hpp

exec_plan: $(OBJECTS)
	$(CC) $(CFLAGS) -o $(EXEC) $(OBJECTS) $(LIBS)

main.o: src/main.cpp
	$(CC) $(CFLAGS) -c src/main.cpp

functions.o: src/functions.cpp
	$(CC) $(CFLAGS) -c src/functions.cpp

.PHONY: clean

clean:
	rm -f $(EXEC) $(OBJECTS)

count:
	wc $(SOURCES) $(HEADERS)