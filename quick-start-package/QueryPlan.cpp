#include <set>
#include <unordered_map>
#include <math.h>
#include "QueryPlan.hpp"

using namespace std;

void ColumnInfo::print() {
    cerr << "  min: "      << this->min << endl;
    cerr << "  max: "      << this->max << endl;
    cerr << "  size: "     << this->size << endl;
    cerr << "  distinct: " << this->distinct << endl;
    cerr << "  n: "        << this->n << endl;
    cerr << "  spread: "   << this->spread << endl << endl;
    flush(cerr);
}

// Estimates the new info of a node's column
// after a filter predicate is applied to that column
void JoinTreeNode::estimateInfoAfterFilter(FilterInfo& filterInfo) {
    if (filterInfo.comparison == FilterInfo::Comparison::Less) {
        this->columnInfo = estimateInfoAfterFilterLess(filterInfo.constant);
    }
    else if (filterInfo.comparison == FilterInfo::Comparison::Greater) {
        this->columnInfo = estimateInfoAfterFilterGreater(filterInfo.constant);
    }
    else if (filterInfo.comparison == FilterInfo::Comparison::Equal) {
        this->columnInfo = estimateInfoAfterFilterEqual(filterInfo.constant);
    }
}

// Returns the new column info
ColumnInfo JoinTreeNode::estimateInfoAfterFilterLess(const int constant) {
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = this->columnInfo.min;
    newColumnInfo.max      = constant;
    newColumnInfo.distinct = (newColumnInfo.max - newColumnInfo.min) / this->columnInfo.spread;
    newColumnInfo.size     = newColumnInfo.distinct * (this->columnInfo.size / this->columnInfo.distinct);
    newColumnInfo.n        = newColumnInfo.max - newColumnInfo.min + 1;
    newColumnInfo.spread   = floor(newColumnInfo.n / newColumnInfo.distinct);

    return newColumnInfo;
}

// Returns the new column info
ColumnInfo JoinTreeNode::estimateInfoAfterFilterGreater(const int constant) {
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = constant;
    newColumnInfo.max      = this->columnInfo.max;
    newColumnInfo.distinct = (newColumnInfo.max - newColumnInfo.min) / this->columnInfo.spread;
    newColumnInfo.size     = newColumnInfo.distinct * (this->columnInfo.size / this->columnInfo.distinct);
    newColumnInfo.n        = newColumnInfo.max - newColumnInfo.min + 1;
    newColumnInfo.spread   = floor(newColumnInfo.n / newColumnInfo.distinct);

    return newColumnInfo;
}

// Returns the new column info
ColumnInfo JoinTreeNode::estimateInfoAfterFilterEqual(const int constant) {
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = constant;
    newColumnInfo.max      = constant;
    newColumnInfo.distinct = 1;
    newColumnInfo.size     = this->columnInfo.size / this->columnInfo.distinct;
    newColumnInfo.n        = 1;
    newColumnInfo.spread   = 1;

    return newColumnInfo;
}

// Construct a plan tree from set of relations IDs
JoinTree* JoinTree::build(QueryInfo& queryInfo, ColumnInfo** columnInfos) {
    // Maps every possible set of relations to its respective best plan tree
    unordered_map< vector<bool>, JoinTree* > BestTree;
    int relationsCount = queryInfo.relationIds.size();

    // Initialise the BestTree structure with nodes
    // for every single relation in the input
    for (int i = 0; i < relationsCount; i++) {
        // Allocate memory
        JoinTree* joinTreePtr = (JoinTree*) malloc(sizeof(JoinTree));
        JoinTreeNode* joinTreeNodePtr = (JoinTreeNode*) malloc(sizeof(JoinTreeNode));

        // Initialise JoinTreeNode
        joinTreeNodePtr->nodeId = queryInfo.relationIds[i]; // The true id of the relation
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
    for (int i=0; i < queryInfo.filters.size(); i++) {
        // Update the tree (containing a single node)
        // of the relation whose column will be filtered
        vector<bool> relationToVector(relationsCount, false);
        relationToVector[queryInfo.filters[i].filterColumn.binding] = true;

        const int relationId = queryInfo.filters[i].filterColumn.relId;
        const int columnId = queryInfo.filters[i].filterColumn.colId;
        BestTree[relationToVector]->root->filterPtr = &(queryInfo.filters[i]);
        BestTree[relationToVector]->root->columnInfo = columnInfos[relationId][columnId];

        // Update the column info
        BestTree[relationToVector]->root->estimateInfoAfterFilter(queryInfo.filters[i]);
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
    //vector<bool> rootToVector(relationsCount, true);
    //return BestTree[rootToVector];

    return NULL;
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
    /* traverse join-tree in a DFS fassion and estimate a cost for every node,
        then sum up this cost and return it */
    int total_cost = 0;
    JoinTreeNode *currNodePtr = joinTreePtr->root;
    bool from_left = true, went_left = false, went_right = false;

    while(currNodePtr) {
        /* if you can go to the left children */
        if (!went_left && currNodePtr->left) {
            currNodePtr = currNodePtr->left;
            from_left = true;
            /* you may now go left or right again */
            went_left = false;
            went_right = false;
        }
        /* if you can go to the right children */
        else if (!went_right && currNodePtr->right) {
            currNodePtr = currNodePtr->right;
            from_left = false;
            /* you may now go left or right again */
            went_left = false;
            went_right = false;
        }
        /* if you can't go to the left or to the right children */
        else {
            /* print node ID */
            total_cost += currNodePtr->cost();
            /* go to the parent */
            currNodePtr = currNodePtr->parent;
            /* deside liberty of transitions */
            if (from_left) {
                went_left = true;
                went_right = false;
            }
            else {
                went_left = true;
                went_right = true;
            }
        }
    }

    return total_cost;
}

// Estimates the cost of a given Join Tree node
double JoinTreeNode::cost() {
    return 1.0;
}

// Builds a query plan with the given info
void QueryPlan::build(QueryInfo& queryInfo) {
#ifdef k
    /* a map that maps each relation-ID to a pointer to it's respective table-type */
    map<RelationId, table_t*> tableTPtrMap;

    /* apply all filters and create a vector of table-types from the query selections */
    for (vector<FilterInfo>::iterator it = queryInfo.filters.begin(); it != queryInfo.filters.end(); ++it) {
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
    for (vector<PredicateInfo>::iterator it = queryInfo.predicates.begin(); it != queryInfo.predicates.end(); ++it) {
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
    for (vector<int>::iterator it = queryInfo.filters.begin() ; it != queryInfo.filters.end(); ++it) {}

    /* create an array of sets of relations to be joined */

    /* create an array of Join Trees for each set of relations */

    /* merge all Join Trees to one final Join Tree */
#endif
}

// Executes a query plan with the given info
void QueryPlan::execute(QueryInfo& queryInfoPtr) {

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
            columnInfos[rel][col].n = maximum - minimum + 1;
            columnInfos[rel][col].spread = floor((maximum - minimum + 1) / (columnInfos[rel][col].distinct));
        }
    }
}
