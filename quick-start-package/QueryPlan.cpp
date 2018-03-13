#include <set>
#include "QueryPlan.hpp"

// The Plan Tree Node constructor
PlanTreeNode::PlanTreeNode() {}

// Construct plan tree from set of relationship IDs
PlanTree* PlanTree::makePlanTree(std::set<int>& relIdSet) {
    std::unordered_map< std::vector<bool>, PlanTree* > BestTree;

    for (int i = 0; i < relIdSet.size(); i++) {
        PlanTree* planTreePtr = (PlanTree*) malloc(1 * sizeof(PlanTree));
        PlanTreeNode* PlanTreeNodePtr = (PlanTreeNode*) malloc(1 * sizeof(PlanTreeNode));
        planTreePtr->root = PlanTreeNodePtr;
        std::vector<bool> tempVec(relIdSet.size(), false);
        tempVec[i] = true;
        BestTree[tempVec] = planTreePtr;
    }

    // generate power-set of given set
    /* source: https://www.geeksforgeeks.org/power-set/ */
    std::map< int, std::set< std::set<int> > > powerSetMap;
    for (int j = 1; j <= relIdSet.size(); j++) {
        unsigned int powerSetSize = pow(2, relIdSet.size());
        std::set< std::set<int> > tempSetOfSets;
        for (int counter = 0; counter < powerSetSize; counter++) {
            std::set<int> tempSet;
            for (int k = 0; k < j; k++) {
                if (counter & (1 << k))
                    tempSet.insert(k);
            }
            tempSetOfSets.insert(tempSet);
        }
        powerSetMap[j] = tempSetOfSets;
    }

    for (int i = 0; i < relIdSet.size()-1; i++) {
        for (auto s : powerSetMap[i]) {
            for (int j = 0; j < relIdSet.size(); j++) {
                // if j not in s
                if (s.find(j) == s.end()) {
                    std::set<int> tempSet;
                    tempSet.insert(j);
                    if (!connected(tempSet, s))
                        continue;

                    std::vector<bool> tempVecS(relIdSet.size(), false);
                    for (auto i : s) tempVecS[i] = true;
                    PlanTree* currTree = makePlanTree(BestTree[tempVecS], j);

                    std::set<int> s1 = s;
                    s1.insert(j);
                    std::vector<bool> tempVecS1(relIdSet.size(), false);
                    for (auto i : s1) tempVecS1[i] = true;
                    if (BestTree.at(tempVecS1) == NULL || cost(BestTree.at(tempVecS1)) > cost(currTree))
                        BestTree.at(tempVecS1) = currTree;
                }
            }
        }
    }

    std::vector<bool> tempVecS(relIdSet.size(), true);
    return BestTree.at(tempVecS);
}

// returns true, if there is a join predicate between one of the relations in its first argument and one of the relations in its second
bool PlanTree::connected(std::set<int>& set1, std::set<int>& set2) {
    return true;
}

// Adds a relationship to a join tree
PlanTree* PlanTree::makePlanTree(PlanTree* left, int relId) {
    return NULL;
}

// execute the plan described by a Plan Tree
void PlanTree::executePlan(PlanTree* planTreePtr) {}

// Destoys a Plan Tree properly
void PlanTree::freePlanTree(PlanTree* planTreePtr) {}

// Prints a Plan Tree -- for debugging
void PlanTree::printPlanTree(PlanTree* planTreePtr) {}

// Estimates the cost of a given Plan Tree */
double PlanTree::cost(PlanTree* planTreePtr) {
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
