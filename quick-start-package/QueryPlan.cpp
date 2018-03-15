#include <set>
#include "QueryPlan.hpp"
#include <unordered_map>

using namespace std;

// Construct a plan tree from set of relations IDs
JoinTree* JoinTree::build(QueryInfo& queryInfoPtr) {
    // Maps every possible set of relations to its respective best plan tree
    unordered_map< vector<bool>, JoinTree* > BestTree;
    int relationsCount = queryInfoPtr.relationIds.size();

    // Initialise the BestTree structure with nodes
    // for every single relation in the input
    for (int i = 0; i < relationsCount; i++) {
        // Allocate memory
        JoinTree* joinTreePtr = (JoinTree*) malloc(sizeof(JoinTree));
        JoinTreeNode* joinTreeNodePtr = (JoinTreeNode*) malloc(sizeof(JoinTreeNode));
        
        // Initialise JoinTreeNode
        joinTreeNodePtr->nodeId = queryInfoPtr.relationIds[i]; // The true id of the relation
        joinTreeNodePtr->left = NULL;
        joinTreeNodePtr->right = NULL;
        joinTreeNodePtr->parent = NULL;
        joinTreeNodePtr->predicatePtr = NULL;
        joinTreeNodePtr->filterPtr = NULL;

        // Initialise JoinTree
        joinTreePtr->root = joinTreeNodePtr;

        // Insert into the BestTree
        vector<bool> relationToVector(relationsCount, false);
        relationToVector[i] = true;
        BestTree[relationToVector] = joinTreePtr;
    }

    // Maps all sets of a certain size to their size
    // Sets are represented as vector<bool>
    map< int, set< vector<bool> > > powerSetMap;

    // Generate power-set of the given set of relations
    // source: www.geeksforgeeks.org/power-set/
    unsigned int powerSetSize = pow(2, relationsCount);

    for (int counter = 0; counter < powerSetSize; counter++) {
        vector<bool> tempVec(relationsCount, false);
        int setSize = 0;

        for (int j = 0; j < relationsCount; j++) {
            if (counter & (1 << j)) {
                tempVec[j] = true;
                setSize++;
            }
        
            // Save all sets of a certain size
            powerSetMap[setSize].insert(tempVec);
        }
    }
/*
    // Print the power set
    fprintf(stderr, "\nrelations = ");
    for (int i = 0; i < relationsCount; i++) fprintf(stderr, "%d ", relationIds[i]);
    fprintf(stderr, "\n");

    for (int i = 1; i <= relationsCount; i++) {
        fprintf(stderr, "set size = %d contains sets = {", i);
        for (auto s : powerSetMap[i]) {
            fprintf(stderr, "{");
            for (int j = 0; j < relationsCount; j++) {
                if (s[j] == true) {
                    fprintf(stderr, " %d ", relationIds[j]);
                }
            }
            fprintf(stderr, "}");
        }
        fprintf(stderr, "}\n");
    }
*/
    
    // Apply all the filters first
    for (int i=0; i < queryInfoPtr.filters.size(); i++) {
        // Update the tree (containing a single node)
        // of the relation whose column will be filtered
        vector<bool> relationToVector(relationsCount, false);
        relationToVector[queryInfoPtr.filters[i].filterColumn.relId] = true;
        BestTree[relationToVector]->root->filterPtr = &(queryInfoPtr.filters[i]);
    }

    // Dynamic programming algorithm
    for (int i = 1; i <= relationsCount; i++) {
        for (auto s : powerSetMap[i]) {
            for (int j = 0; j < relationsCount; j++) {
                // If j is not in the set
                if (s[j] == false) {
                    // Create the bit vector representation of the relation j
                    vector<bool> relationToVector(relationsCount, false);
                    relationToVector[j] = true;
                    
                    // Merge the two trees
                    JoinTree* currTree = CreateJoinTree(BestTree[s], BestTree[relationToVector]);
                    
                    // Save the new merged tree
                    vector<bool> s1 = s;
                    s1[j] = true;
                    if (BestTree[s1] == NULL || cost(BestTree[s1]) > cost(currTree)) {
                        BestTree[s1] = currTree;
                    }
                }
            }
        }
    }

    // Return the best tree in the root
    vector<bool> rootToVector(relationsCount, true);
    return BestTree[rootToVector];
}

// returns true, if there is a join predicate between one of the relations in its first argument
// and one of the relations in its second
/*
bool JoinTree::connected(int relId, set<int>& idSet, set<PredicateInfo>& predSet) {
    for (auto p : predSet)
        for (auto s : idSet)
            if ((p.left.relId == relId && p.right.relId == s) && (p.left.relId == s && p.right.relId == relId))
                return true;

    return false;
}
*/

// Merges two join trees
JoinTree* JoinTree::CreateJoinTree(JoinTree* leftTree, JoinTree* rightTree) {
    // Allocate memory for the new tree
    JoinTree* joinTreePtr = (JoinTree*) malloc(sizeof(JoinTree));
    JoinTreeNode* joinTreeNodePtr = (JoinTreeNode*) malloc(sizeof(JoinTreeNode));

    // Initialise the new JoinTreeNode
    joinTreeNodePtr->nodeId = -1; // This is an intermediate node
    joinTreeNodePtr->left = leftTree->root;
    joinTreeNodePtr->right = rightTree->root;
    joinTreeNodePtr->parent = NULL;
    joinTreeNodePtr->predicatePtr = NULL;
    joinTreeNodePtr->filterPtr = NULL;

    // Initialise the new JoinTree
    joinTreePtr->root = joinTreeNodePtr;

    // Update the parent pointers of the merged trees
    leftTree->root->parent = joinTreePtr->root;
    rightTree->root->parent = joinTreePtr->root;

    // Estimate the new info of the merged columns

    return joinTreePtr;
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
    map<RelationId, table_t*> tableTPtrMap;

    /* apply all filters and create a vector of table-types from the query selections */
    for (vector<FilterInfo>::iterator it = queryInfoPtr.filters.begin(); it != queryInfoPtr.filters.end(); ++it) {
        map<RelationId, table_t*>::iterator mapIt = tableTPtrMap.find(it->filterColumn.relId);

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
    set<table_t*> tableTPtrSet;
    for (vector<PredicateInfo>::iterator it = queryInfoPtr.predicates.begin(); it != queryInfoPtr.predicates.end(); ++it) {
        map<RelationId, table_t*>::iterator mapItLeft = tableTPtrMap.find(it->left.relId),
                                            mapItRight = tableTPtrMap.find(it->right.relId);
        /* sanity check -- REMOVE in the end */
        assert(mapItLeft != tableTPtrMap.end() && mapItRight != tableTPtrMap.end());

        tableTPtrSet.insert(mapItLeft);
        tableTPtrSet.insert(mapItRight);
    }

    /* create a Join Tree by joining all the table-types of the set */
    JoinTree* tempJoinTreePtr = constrJoinTreeFromRelations(tableTPtrSet);

    /* apply all filters */
    for (vector<int>::iterator it = queryInfoPtr.filters.begin() ; it != queryInfoPtr.filters.end(); ++it) {}

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

        cerr << "relation: " << rel << endl;
        flush(cerr);

        // Get the info of every column
        for (int col = 0; col < columnsCount; col++) {
            uint64_t minimum = numeric_limits<uint64_t>::max(); // Value of minimum element
            uint64_t maximum = 0; // Value of maximum element
            set<uint64_t> distinctElements; // Keep the distinct elements of the column

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

            cerr << "    column: " << col << endl;
            cerr << "    min: " << columnInfos[rel][col].min << endl;
            cerr << "    max: " << columnInfos[rel][col].max << endl;
            cerr << "    size: " << columnInfos[rel][col].size << endl;
            cerr << "    distinct: " << columnInfos[rel][col].distinct << endl << endl;
            flush(cerr);
        }
    }
}
