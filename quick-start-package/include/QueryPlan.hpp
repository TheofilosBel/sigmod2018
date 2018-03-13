#pragma once

#include <cstdint>
#include <limits>
#include <iostream>
#include <map>
#include <set>
#include <math.h>
#include "Joiner.hpp"

// Keeps the important info/statistics for every column
// needed to build the plan tree
struct ColumnInfo {
    uint64_t min; // Value of the minimum element
    uint64_t max; // Value of the maximum element
    uint64_t size; // Total number of elements
    uint64_t distinct; // Number of distinct elements
};

// Plan Tree's node
struct PlanTreeNode {
    unsigned nodeID;

    PlanTreeNode* left;
    PlanTreeNode* right;
    PlanTreeNode* parent;

    PredicateInfo* predicateInfoPtr;
    FilterInfo* filterInfoPtr;

    ColumnInfo* intermediateColumnInfoPtr;

    // The constructor
    PlanTreeNode();
};

// Plan Tree data structure
struct PlanTree {
    PlanTreeNode* root;

    bool isRightDeepOnly;
    bool isLeftDeepOnly;

    // Construct plan tree from set of relationship IDs
    PlanTree* makePlanTree(std::set<int>& relIdSet, std::set<PredicateInfo>& predSet);

    // returns true, if there is a join predicate between one of the relations in its first argument and one of the relations in its second
    bool connected(int relId, std::set<int>& idSet, std::set<PredicateInfo>& predSet);

    // Adds a relationship to a join tree
    PlanTree* makePlanTree(PlanTree* left, int relId);

    // execute the plan described by a Plan Tree
    void executePlan(PlanTree* planTreePtr);

    // Destoys a Plan Tree properly
    void freePlanTree(PlanTree* planTreePtr);

    // Prints a Plan Tree -- for debugging
    void printPlanTree(PlanTree* planTreePtr);

    // Estimates the cost of a given Plan Tree
    double cost(PlanTree* planTreePtr);
};

// Query Plan data structure
struct QueryPlan {
    // Keeps the info of every column of every relation
    // Every row represents a relation
    // Every item of a row represents a column of the relation
    ColumnInfo** columnInfos;

    PlanTree* planTreePtr; // The plan tree to execute

    // The constructor
    //QueryPlan()

    // Build a query plan with the given info
    void build(QueryInfo& queryInfoPtr);

    // Fills the columnInfo matrix with the data of every column
    void fillColumnInfo(Joiner& joiner);
};

/*
// destruct a Join Tree node properly
void destrJoinTreeNode(JoinTreeNode* joinTreeNodePtr);

// print a Join Tree node -- for debugging
void printJoinTreeNode(JoinTreeNode* joinTreeNodePtr);

// estimate the cost of a given Join Tree node
double joinTreeNodeCost(JoinTreeNode* joinTreeNodePtr);
*/
