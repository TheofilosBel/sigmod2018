OBJS 	= joiner_main.o joiner.o
SOURCE	= joiner_main.cpp joiner.cpp
HEADER  = joiner.hpp
OUT  	= join
CC		= g++
FLAGS   = -g3 -c -std=c++11
#-g3 for valgrind
all: $(OBJS)
	$(CC) $(OBJS) -o $(OUT)

# create/compile the individual files >>separately<<
joiner_main.o: joiner_main.cpp
	$(CC) $(FLAGS) joiner_main.cpp

joiner.o: joiner.cpp
	$(CC) $(FLAGS) joiner.cpp

# clean house
clean:
	rm -f $(OBJS) $(OUT)

# do a bit of accounting
count:
	wc $(SOURCE) $(HEADER)