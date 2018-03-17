#pragma once
#include <cstdint>
#include <vector>
#include "Parser.hpp"
#include "Relation.hpp"

struct QueryGraphNode {
    int exists; // 0 or 1
    unsigned columnLeft; // The column of the left hand side of the predicate
    unsigned columnRight; // The column of the right hand side of the predicate

    // The constructor
    QueryGraphNode() : exists(0), columnLeft(0), columnRight(0) {};
};

struct Edge {
    RelationId relationLeft; // The relation id of left hand side of the predicate
    unsigned columnLeft; // The column of the left hand side of the predicate

    RelationId relationRight; // The relation id of right hand side of the predicate
    unsigned columnRight; // The column of the right hand side of the predicate

    // Default constructor
    Edge() : relationLeft(0), columnLeft(0), relationRight(0), columnRight(0) {};

    // The constructor
    Edge(RelationId relationLeft, unsigned columnLeft, RelationId relationRight, unsigned columnRight) :
        relationLeft(relationLeft), columnLeft(columnLeft), relationRight(relationRight), columnRight(columnRight) {};
};

struct QueryGraph {
    std::vector<std::vector<QueryGraphNode> > graph; // The graph is represented as a 2d matrix
    std::vector<Edge> edges; // The edges of the graph connecting the nodes on join predicates

    // Default constructor
    QueryGraph() {};

    // Constructor that allocates memory for the graph
    QueryGraph(int dimensions);

    // Resets all elements of the graph to zeros
    void clear();

    // Fills the query graph with the info of a query
    void fill(std::vector<PredicateInfo> &predicates);

    // Prints a graph in a readable format
    void printGraph(int dimensions);
};
