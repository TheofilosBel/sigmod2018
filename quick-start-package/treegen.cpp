#include "./include/header.hpp"

#include <set>
#include <map>

#include <cstdio>

JTree *treegen(QueryInfo *info)
{

	using namespace std;

	map<int,JTree*> worklist;

	map<JTree*,set<int>*> nodeSets;

	JTree *node;

	/*add all the nodes to the worklist*/
	for (unsigned int i=0; i < info->relationIds.size(); i++) {
		node = new JTree;
		node->node_id = i;
		node->predPtr = NULL;
		node->predPtr = NULL;



		node->right = NULL;
		node->left = NULL;
		node->parent = NULL;
		worklist[node->node_id] = node;

		nodeSets[node]= new set<int>;
		nodeSets[node]->insert(i);
	}
	//Apply all fitlers
	for (unsigned int i=0; i < info->filters.size(); i++) {

		node = new JTree;
		node->node_id = -1;
		node->filterPtr = &(info->filters[i]);
		node->predPtr = NULL;

		node->right = NULL;
		node->left = NULL;
		node->parent = NULL;


		node->left = worklist[info->filters[i].filterColumn.binding];

		for (set<int>::iterator it=nodeSets[node->left]->begin(); it != nodeSets[node->left]->end(); ++it) {

			worklist[*it] = node;
		}
		nodeSets[node] = nodeSets[node->left];


		node->left->parent = node;



	}

	/*apply the constraints */
	for (unsigned int i=0; i < info->predicates.size(); i++) {

		node = new JTree;
		node->node_id = -1;
		node->predPtr = &(info->predicates[i]);
		node->filterPtr = NULL;

		node->right = NULL;
		node->left = NULL;
		node->parent = NULL;





		node->left = worklist[info->predicates[i].left.binding];
		node->right = worklist[info->predicates[i].right.binding];


		for (set<int>::iterator it=nodeSets[node->left]->begin(); it != nodeSets[node->left]->end(); ++it) {

			worklist[*it] = node;
		}
		nodeSets[node] = nodeSets[node->left];

		node->left->parent = node;




		if (node->right == node->left)
			node->right = NULL;
		else {


			for (set<int>::iterator it=nodeSets[node->right]->begin(); it != nodeSets[node->right]->end(); ++it) {

				worklist[*it] = node;
				nodeSets[node]->insert(*it);

			}
			delete nodeSets[node->right];


			node->right->parent = node;
		}

	}

	/*join the remainiing tables , those for which parent = null*/

	set<JTree*> tables;
	for (map<int, JTree*>::iterator it=worklist.begin(); it!=worklist.end(); ++it) {

		if (it->second->parent == NULL) {
			tables.insert(it->second);
		}
	}



	vector<JTree*> v(tables.begin(), tables.end());

	JTree *top = v[0];
	delete nodeSets[v[0]];
	for (int i=0; i < v.size() -1; i++) {

		node = new JTree;
		node->node_id = -1;
		node->filterPtr = NULL;
		node->predPtr = NULL;

		node->left = top;
		node->right = v[i+1];
		delete nodeSets[v[i+1]];
		top = node;


	}
	return top;



}
