#include <iostream>
#include "Utils.hpp"

using namespace std;

// Create a dummy column
static void createColumn(vector<uint64_t*>& columns,uint64_t numTuples) {
    auto col=new uint64_t[numTuples];
    columns.push_back(col);
    for (unsigned i=0;i<numTuples;++i) {
        col[i]=i;
    }
}

// Create a dummy relation
Relation Utils::createRelation(uint64_t size,uint64_t numColumns) {
    vector<uint64_t*> columns;
    for (unsigned i=0;i<numColumns;++i) {
        createColumn(columns,size);
    }
    return Relation(size,move(columns));
}

// Store a relation in all formats
void Utils::storeRelation(ofstream& out,Relation& r,unsigned i) {
    auto baseName = "r" + to_string(i);
    r.storeRelation(baseName);
    r.storeRelationCSV(baseName);
    r.dumpSQL(baseName, i);
    cout << baseName << "\n";
    out << baseName << "\n";
}
