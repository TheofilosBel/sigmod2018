#pragma once

#include "header_NEW.hpp"

/* Join Tree's data structures */

/* Join Tree's node */
typedef struct JoinTreeNode {

} JoinTreeNode;

/* Join Tree */
typedef struct JoinTree {
  JoinTreeNode* rootPtr;

  bool isRightDeepOnly;
  bool isLeftDeepOnly;

} JoinTree;

/* Query Plan's structure -- holds whatever is important for the plan's description */
typedef struct QueryPlan {
  JoinTree* joinTreePtr;

  vector<vector<RelationStatistics>> relStats;

} QueryPlan;

/* complementary functions */

/* construct a Query Plan -- do all the pre-processing that is necessary for a
  Query to be translated into a Join Tree and then start the whole process */
QueryPlan* constrQueryPlan(QueryInfo* queryInfoPtr);

/* construct an optimal left-deep Join Tree from a given set of relations */
JoinTree* constrJoinTreeFromRelations(std::vector<Relation>& relations);

/* construct an optimal Join Tree from two given optimal Join Trees */
JoinTree* constrJoinTreeFromJoinTrees(JoinTree* joinTreePtr1, JoinTree* joinTreePtr2);

/* construct a Join Tree node */
JoinTreeNode* constrJoinTreeNode();

/* destruct a Join Tree properly */
void destrJoinTree(JoinTree* joinTreePtr);

/* destruct a Join Tree node properly */
void destrJoinTreeNode(JoinTreeNode* joinTreeNodePtr);

/* print a Join Tree -- for debugging */
void printJoinTree(JoinTree* joinTreePtr);

/* print a Join Tree node -- for debugging */
void printJoinTreeNode(JoinTreeNode* joinTreeNodePtr);

/* estimate the cost of a given Join Tree */
double joinTreeCost(JoinTree* joinTreePtr);

/* estimate the cost of a given Join Tree node */
double joinTreeNodeCost(JoinTreeNode* joinTreeNodePtr);
