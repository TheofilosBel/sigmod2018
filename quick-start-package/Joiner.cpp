#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Parser.hpp"

using namespace std;
//---------------------------------------------------------------------------
void Joiner::addRelation(const char* fileName)
// Loads a relation from disk
{
   relations.emplace_back(fileName);
}
//---------------------------------------------------------------------------
Relation& Joiner::getRelation(unsigned relationId)
// Loads a relation from disk
{
   if (relationId >= relations.size()) {
      cerr << "Relation with id: " << relationId << " does not exist" << endl;
      throw;
   }
   return relations[relationId];
}
//---------------------------------------------------------------------------
void Joiner::join(QueryInfo& i)
// Hashes a value and returns a check-sum
// The check should be NULL if there is no qualifying tuple
{
  //TODO implement
  // print result to std::cout
  cout << "Implement join..." << endl;
}
//---------------------------------------------------------------------------
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
