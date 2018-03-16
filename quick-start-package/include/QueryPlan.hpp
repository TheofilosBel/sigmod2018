#pragma once

#include <cstdint>
#include <limits>
#include <iostream>
#include <map>
#include <set>
#include <math.h>
#include "Joiner.hpp"
#include "Parser.hpp"

// Keeps the important info/statistics for every column
// needed to build the plan tree
struct ColumnInfo {
    uint64_t min; // Value of the minimum element
    uint64_t max; // Value of the maximum element
    uint64_t size; // Total number of elements
    uint64_t distinct; // Number of distinct elements
    uint64_t n; // The size of the domain
    uint64_t spread; // The spread of the values in the domain

    // Prints a Column Info structure
    void print();
};

// Join Tree's node
struct JoinTreeNode {
    unsigned nodeId;

    JoinTreeNode* left;
    JoinTreeNode* right;
    JoinTreeNode* parent;

    PredicateInfo* predicatePtr;
    FilterInfo* filterPtr;
    ColumnInfo columnInfo;

    // Estimates the new info of a node's column
    // after a filter predicate is applied to that column
    void estimateInfoAfterFilter(FilterInfo& filterInfo);

    // Returns the new column info
    ColumnInfo estimateInfoAfterFilterLess(const int constant);
    ColumnInfo estimateInfoAfterFilterGreater(const int constant);
    ColumnInfo estimateInfoAfterFilterEqual(const int constant);

    // Estimates the cost of a given Join Tree node
    double cost();
};

// Join Tree data structure
struct JoinTree {
    JoinTreeNode* root;
    double treeCost; // The total cost of the join tree

    // Constructs a Join tree from a set of relations id's
    JoinTree* build(QueryInfo& queryInfoPtr, ColumnInfo** columnInfos);

    // returns true, if there is a join predicate between one of the relations in its first argument
    //and one of the relations in its second
    //bool connected(int relId, std::set<int>& idSet, std::set<PredicateInfo>& predSet);

    // Merges two join trees
    JoinTree* CreateJoinTree(JoinTree* leftTree, JoinTree* rightTree);

    // Execute a Join Tree
    void execute(JoinTree* joinTreePtr);

    // Destoys a Join Tree properly
    void freeJoinTree(JoinTree* joinTreePtr);

    // Prints a Join Tree -- for debugging
    void printJoinTree(JoinTree* joinTreePtr);

    // Estimates the cost of a given Join Tree
    double cost(JoinTree* joinTreePtr);
};

// Query Plan data structure
struct QueryPlan {
    // Keeps the info of every column of every relation
    // Every row represents a relation
    // Every item of a row represents a column of the relation
    ColumnInfo** columnInfos;

    JoinTree* joinTreePtr; // The plan tree to execute

    // Build a query plan with the given info
    void build(QueryInfo& queryInfoPtr);

    // Execute a query plan with the given info
    void execute(QueryInfo& queryInfoPtr);

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
