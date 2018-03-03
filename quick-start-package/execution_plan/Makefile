CC = g++-6
CFLAGS = -Wall -std=c++11 -g3
LIBS = -lm
EXEC = exec_plan
OBJECTS = main.o functions.o Parser.o treegen.o
SOURCES = src/main.cpp src/functions.cpp src/reegen.cpp
HEADERS = include/header.hpp include/functions.hpp

exec_plan: $(OBJECTS)
	$(CC) $(CFLAGS) -o $(EXEC) $(OBJECTS) $(LIBS)

main.o: src/main.cpp
	$(CC) $(CFLAGS) -c src/main.cpp

functions.o: src/functions.cpp
	$(CC) $(CFLAGS) -c src/functions.cpp


Parser.o: ../Parser.cpp
	$(CC) $(CFLAGS) -c Parser.cpp -o Parser.o

treegen.o: src/treegen.cpp
	$(CC) $(CFLAGS) -c src/treegen.cpp -o treegen.o



.PHONY: clean

clean:
	rm -f $(EXEC) $(OBJECTS)

count:
	wc $(SOURCES) $(HEADERS)
