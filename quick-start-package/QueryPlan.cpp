#include <set>
#include "QueryPlan.hpp"
#include <unordered_map>

// Construct plan tree from set of relations IDs
JoinTree* JoinTree::build(std::vector<RelationId>& relationIds, std::vector<PredicateInfo>& predicates) {
    // Maps every possible set of relations to its respective best plan tree
    std::unordered_map< std::vector<bool>, JoinTree* > BestTree;
    int relationsCount = relationIds.size();

    // Initialise the BestTree structure
    for (int i = 0; i < relationsCount; i++) {
        // Allocate memory
        JoinTree* joinTreePtr = (JoinTree*) malloc(sizeof(JoinTree));
        JoinTreeNode* joinTreeNodePtr = (JoinTreeNode*) malloc(sizeof(JoinTreeNode));
        
        // Initialise JoinTreeNode
        joinTreeNodePtr->nodeId = relationIds[i]; // The true id of the relation
        joinTreeNodePtr->left = NULL;
        joinTreeNodePtr->right = NULL;
        joinTreeNodePtr->parent = NULL;
        joinTreeNodePtr->predicateInfoPtr = NULL;
        joinTreeNodePtr->filterInfoPtr = NULL;
        joinTreeNodePtr->intermediateColumnInfoPtr = NULL;

        // Initialise JoinTree
        joinTreePtr->root = joinTreeNodePtr;
        joinTreePtr->isRightDeepOnly = false;
        joinTreePtr->isLeftDeepOnly = true;

        // Insert into the BestTree
        std::vector<bool> tempVec(relationsCount, false);
        tempVec[i] = true;
        BestTree[tempVec] = joinTreePtr;
    }

    // Maps all sets of a certain size to their size
    std::map< int, std::set< std::set<int> > > powerSetMap;

    // Generate power-set of the given set of relations
    // source: www.geeksforgeeks.org/power-set/
    unsigned int powerSetSize = pow(2, relationsCount);

    for (int counter = 0; counter < powerSetSize; counter++) {
        std::set<int> tempSet;
        for (int j = 0; j < relationsCount; j++) {
            if (counter & (1 << j)) {
                tempSet.insert(j);
            }
        
            // Save all sets of a certain size
            powerSetMap[j].insert(tempSet);
        }
    }

    // Dynamic programming algorithm
    for (int i = 1; i <= relationsCount; i++) {
        for (auto s : powerSetMap[i]) {
            for (int j = 0; j < relationsCount; j++) {
                // If j is not in the set
                if (s.find(j) == s.end()) {
                    //if (!connected(j, s, predicates))
                    //    continue;

                    // Create the bit vector representation of the set
                    std::vector<bool> setToVector(relationsCount, false);
                    for (auto i : s) setToVector[i] = true;
                    JoinTree* currTree = CreateJoinTree(BestTree[setToVector], j);

                    std::set<int> s1 = s;
                    s1.insert(j);
                    std::vector<bool> tempVecS1(relationsCount, false);
                    for (auto i : s1) tempVecS1[i] = true;
                    if (BestTree.at(tempVecS1) == NULL || cost(BestTree.at(tempVecS1)) > cost(currTree))
                        BestTree.at(tempVecS1) = currTree;
                }
            }
        }
    }

    std::vector<bool> setToVector(relationsCount, true);
    return BestTree.at(setToVector);
}

// returns true, if there is a join predicate between one of the relations in its first argument
// and one of the relations in its second
/*
bool JoinTree::connected(int relId, std::set<int>& idSet, std::set<PredicateInfo>& predSet) {
    for (auto p : predSet)
        for (auto s : idSet)
            if ((p.left.relId == relId && p.right.relId == s) && (p.left.relId == s && p.right.relId == relId))
                return true;

    return false;
}
*/

// Adds a relation to a join tree
JoinTree* JoinTree::CreateJoinTree(JoinTree* left, int relId) {
    return NULL;
}

// execute the plan described by a Plan Tree
void JoinTree::execute(JoinTree* joinTreePtr) {}

// Destoys a Plan Tree properly
void JoinTree::freeJoinTree(JoinTree* joinTreePtr) {}

// Prints a Plan Tree -- for debugging
void JoinTree::printJoinTree(JoinTree* joinTreePtr) {}

// Estimates the cost of a given Plan Tree */
double JoinTree::cost(JoinTree* joinTreePtr) {
    return 1.0;
}

// Builds a query plan with the given info
void QueryPlan::build(QueryInfo& queryInfoPtr) {
#ifdef k
    /* a map that maps each relation-ID to a pointer to it's respective table-type */
    std::map<RelationId, table_t*> tableTPtrMap;

    /* apply all filters and create a vector of table-types from the query selections */
    for (std::vector<FilterInfo>::iterator it = queryInfoPtr.filters.begin(); it != queryInfoPtr.filters.end(); ++it) {
        std::map<RelationId, table_t*>::iterator mapIt = tableTPtrMap.find(it->filterColumn.relId);

        /* if table-type for this relation-ID already in map, then update the respective table-type */
        if (mapIt != tableTPtrMap.end()) {
            Select(it, mapIt->second);
        }
        else {
            /* otherwise add the table-type's mapping to the map */
            table_t* tempTableTPtr = CreateTableTFromId(it->filterColumn.relId, it->filterColumn.binding);
            Select(it, tempTableTPtr);
            tableTPtrMap.insert(it->filterColumn.relId, tempTableTPtr);
        }
    }

    /* create a set of pointers to the table-types that are to be joined */
    std::set<table_t*> tableTPtrSet;
    for (std::vector<PredicateInfo>::iterator it = queryInfoPtr.predicates.begin(); it != queryInfoPtr.predicates.end(); ++it) {
        std::map<RelationId, table_t*>::iterator mapItLeft = tableTPtrMap.find(it->left.relId),
                                            mapItRight = tableTPtrMap.find(it->right.relId);
        /* sanity check -- REMOVE in the end */
        assert(mapItLeft != tableTPtrMap.end() && mapItRight != tableTPtrMap.end());

        tableTPtrSet.insert(mapItLeft);
        tableTPtrSet.insert(mapItRight);
    }

    /* create a Join Tree by joining all the table-types of the set */
    JoinTree* tempJoinTreePtr = constrJoinTreeFromRelations(tableTPtrSet);

    /* apply all filters */
    for (std::vector<int>::iterator it = queryInfoPtr.filters.begin() ; it != queryInfoPtr.filters.end(); ++it) {}

    /* create an array of sets of relations to be joined */

    /* create an array of Join Trees for each set of relations */

    /* merge all Join Trees to one final Join Tree */
#endif
}

// Fills the columnInfo matrix with the data of every column
void QueryPlan::fillColumnInfo(Joiner& joiner) {
    Relation* relation;
    int relationsCount = joiner.getRelationsCount(); // Get the number of relations
    int columnsCount; // Number of columns of a relation

    // Allocate memory for every relation
    columnInfos = (ColumnInfo**) malloc(relationsCount * sizeof(ColumnInfo*));

    // For every relation get its column statistics
    for (int rel = 0; rel < relationsCount; rel++) {
        // Get the number of columns
        relation = &(joiner.getRelation(rel));
        columnsCount = relation->columns.size();

        // Allocate memory for the columns
        columnInfos[rel] = (ColumnInfo*) malloc(columnsCount * sizeof(ColumnInfo));

        std::cerr << "relation: " << rel << std::endl;
        flush(std::cerr);

        // Get the info of every column
        for (int col = 0; col < columnsCount; col++) {
            uint64_t minimum = std::numeric_limits<uint64_t>::max(); // Value of minimum element
            uint64_t maximum = 0; // Value of maximum element
            std::set<uint64_t> distinctElements; // Keep the distinct elements of the column

            uint64_t tuples = relation->size;
            for (int i = 0; i < tuples; i++) {
                uint64_t element = relation->columns[col][i];
                if (element > maximum) maximum = element;
                if (element < minimum) minimum = element;
                distinctElements.insert(element);
            }

            // Save the infos
            columnInfos[rel][col].min = minimum;
            columnInfos[rel][col].max = maximum;
            columnInfos[rel][col].size = tuples;
            columnInfos[rel][col].distinct = (uint64_t) distinctElements.size();

            std::cerr << "    column: " << col << std::endl;
            std::cerr << "    min: " << columnInfos[rel][col].min << std::endl;
            std::cerr << "    max: " << columnInfos[rel][col].max << std::endl;
            std::cerr << "    size: " << columnInfos[rel][col].size << std::endl;
            std::cerr << "    distinct: " << columnInfos[rel][col].distinct << std::endl << std::endl;
            flush(std::cerr);
        }
    }
}
