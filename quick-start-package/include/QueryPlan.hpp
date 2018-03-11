#pragma once

#include <cstdint>
#include <iostream>
#include <unordered_map>

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
    PlanTreeNode* left;
    PlanTreeNode* right;
    PlanTreeNode* parent;
};

// Plan Tree data structure
struct PlanTree {
    PlanTreeNode* rootPtr;

    bool isRightDeepOnly;
    bool isLeftDeepOnly;

    // The constructor
    PlanTree();

    // Merges two join trees into one
    // and return the resulted tree
    PlanTree* mergePlanTrees(PlanTree* left, PlanTree* right);

    // Destoys a Plan Tree properly
    void freePlanTree(PlanTree* planTreePtr);

    // Prints a Plan Tree -- for debugging
    void printPlanTree(PlanTree* planTreePtr);

    // Estimates the cost of a given Plan Tree */
    double costOfPlanTree(PlanTree* planTreePtr);
};

// Query Plan data structure
struct QueryPlan {
    vector<vector<ColumnInfo>> columnInfo; // Keeps the info of every column
    PlanTree* planTreePtr; // The plan tree to execute

    // The constructor
    QueryPlan(QueryInfo* queryInfoPtr);
};