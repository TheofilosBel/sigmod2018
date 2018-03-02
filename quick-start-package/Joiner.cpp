#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Parser.hpp"

using namespace std;

// Loads a relation from disk
void Joiner::addRelation(const char* fileName) {
    relations.emplace_back(fileName);
}

// Loads a relation from disk
Relation& Joiner::getRelation(unsigned relationId) {
    if (relationId >= relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return relations[relationId];
}

// Hashes a value and returns a check-sum
// The check should be NULL if there is no qualifying tuple
void Joiner::join(QueryInfo& i) {
    //TODO implement
    // print result to std::cout
    cout << "Implement join..." << endl;
}

int main(int argc, char* argv[]) {
    Joiner joiner;

    // Read join relations
    string line;
    while (getline(cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }
   
    // Preparation phase (not timed)
    // Build histograms, indices,...

    // The test harness will send the first query after 1 second.
    QueryInfo i;
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch
        i.parseQuery(line);
        joiner.join(i);
    }

    return 0;
}
