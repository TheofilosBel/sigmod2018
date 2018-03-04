#include <cassert>
#include <iostream>
#include <cstring>
#include "QueryGraph.hpp"

using namespace std;

// The constructor
QueryGraph::QueryGraph(int dimensions) {
    // Resize the matrix
    graph.resize(dimensions);
    for (auto &it : graph) {
        it.resize(dimensions);
    }
}

// Resets all elements of the graph to zeros
void QueryGraph::clear() {
    for(auto& it : graph) {
        //memset(&it[0], 0, sizeof(QueryGraphNode)*it.size());
        std::fill(it.begin(), it.end(), QueryGraphNode());
    }

    edges.clear();
}

// Fills the query graph with the info of a query
void QueryGraph::fill(std::vector<PredicateInfo> &predicates) {
    for(auto& it : predicates) {
        // Fill the graph
        graph[it.left.relId][it.right.relId].exists = 1;
        graph[it.left.relId][it.right.relId].columnLeft = it.left.colId;
        graph[it.left.relId][it.right.relId].columnRight = it.right.colId;
    
        // Save the edges
        edges.push_back(Edge(it.left.relId, it.left.colId, it.right.relId, it.right.colId));
    }
}

// Prints a graph in a readable format
void QueryGraph::printGraph(int dimensions) {
    printf(" --- ");
    for (int i = 0; i < dimensions; i++) {
        for (int j = 0; j < dimensions; j++) {
            if (graph[i][j].exists == 1) {
                printf("%d.%d=%d.%d ", i, graph[i][j].columnLeft, j, graph[i][j].columnRight);
            }
        }
    }

    printf("--- ");
    for (int i = 0; i < edges.size(); i++) {
        printf("%d.%d=%d.%d ", edges[i].relationLeft, edges[i].columnLeft, edges[i].relationRight, edges[i].columnRight);
    }

    printf("\n");
}
