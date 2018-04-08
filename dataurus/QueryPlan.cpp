#include <bitset>
#include <unordered_set>
#include <math.h>
#include <limits>
#include "QueryPlan.hpp"
#include "parallel_radix_join.h"

using namespace std;

//#define prints

void ColumnInfo::print() {
    cerr << "min:      " << this->min << endl;
    cerr << "max:      " << this->max << endl;
    cerr << "size:     " << this->size << endl;
    cerr << "distinct: " << this->distinct << endl;
    cerr << "n:        " << this->n << endl;
    cerr << "spread:   " << this->spread << endl << endl;
    flush(cerr);
}

// Estimates the new info of a node's column
// after a filter predicate is applied to that column
void JoinTreeNode::estimateInfoAfterFilter(FilterInfo& filterInfo) {
    if (filterInfo.comparison == FilterInfo::Comparison::Less) {
        this->estimateInfoAfterFilterLess(filterInfo);
    }
    else if (filterInfo.comparison == FilterInfo::Comparison::Greater) {
        this->estimateInfoAfterFilterGreater(filterInfo);
    }
    else if (filterInfo.comparison == FilterInfo::Comparison::Equal) {
        this->estimateInfoAfterFilterEqual(filterInfo);
    }
}

// Updates the column info map
void JoinTreeNode::estimateInfoAfterFilterLess(FilterInfo& filterInfo) {
    ColumnInfo oldColumnInfo = this->usedColumnInfos[filterInfo.filterColumn];

    // Update the info of the column on which the filter is applied
    ColumnInfo newColumnInfo;
    newColumnInfo.min      = oldColumnInfo.min;
    newColumnInfo.max      = filterInfo.constant;
    newColumnInfo.distinct = (uint64_t) (((double) (newColumnInfo.max - newColumnInfo.min)) / oldColumnInfo.spread);
    if (newColumnInfo.distinct == 0) newColumnInfo.distinct = 1;
    newColumnInfo.size     = newColumnInfo.distinct * (oldColumnInfo.size / oldColumnInfo.distinct);
    newColumnInfo.n        = newColumnInfo.max - newColumnInfo.min + 1;
    newColumnInfo.spread   = ((double) newColumnInfo.n) / ((double) newColumnInfo.distinct);
    newColumnInfo.counter  = oldColumnInfo.counter - 1;
    newColumnInfo.isSelectionColumn = oldColumnInfo.isSelectionColumn;
    newColumnInfo.updateSize        = oldColumnInfo.updateSize;

    this->usedColumnInfos[filterInfo.filterColumn] = newColumnInfo;

    // Update the info of the other columns
    for (columnInfoMap::iterator it=this->usedColumnInfos.begin(); it != this->usedColumnInfos.end(); it++) {
        if (!(it->first == filterInfo.filterColumn)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            double base      = 1 - (((double) newColumnInfo.size) / ((double) oldColumnInfo.size));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newColumnInfo.size;
            it->second.distinct = (uint64_t) (((double) it->second.distinct) * (1 - tempValue));
            if (it->second.distinct == 0) it->second.distinct = 1;
            it->second.spread   = ((double)it->second.n) / ((double) it->second.distinct);
        }
    }

    // Check if this column is needed anymore
    if ((newColumnInfo.counter == 0) && (newColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(filterInfo.filterColumn);
    }
}

// Updates the column info map
void JoinTreeNode::estimateInfoAfterFilterGreater(FilterInfo& filterInfo) {
    ColumnInfo oldColumnInfo = this->usedColumnInfos[filterInfo.filterColumn];

    // Update the info of the column on which the filter is applied
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = filterInfo.constant;
    newColumnInfo.max      = oldColumnInfo.max;
    newColumnInfo.distinct = (uint64_t) (((double) (newColumnInfo.max - newColumnInfo.min)) / oldColumnInfo.spread);
    if (newColumnInfo.distinct == 0) newColumnInfo.distinct = 1;
    newColumnInfo.size     = newColumnInfo.distinct * (oldColumnInfo.size / oldColumnInfo.distinct);
    newColumnInfo.n        = newColumnInfo.max - newColumnInfo.min + 1;
    newColumnInfo.spread   = ((double) newColumnInfo.n) / ((double) newColumnInfo.distinct);
    newColumnInfo.counter  = oldColumnInfo.counter - 1;
    newColumnInfo.isSelectionColumn = oldColumnInfo.isSelectionColumn;
    newColumnInfo.updateSize        = oldColumnInfo.updateSize;

    this->usedColumnInfos[filterInfo.filterColumn] = newColumnInfo;

    // Update the info of the other columns
    for (columnInfoMap::iterator it=this->usedColumnInfos.begin(); it != this->usedColumnInfos.end(); it++) {
        if (!(it->first == filterInfo.filterColumn)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            double base      = 1 - (((double) newColumnInfo.size) / ((double) oldColumnInfo.size));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newColumnInfo.size;
            it->second.distinct = (uint64_t) (((double) it->second.distinct) * (1 - tempValue));
            if (it->second.distinct == 0) it->second.distinct = 1;
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
        }
    }

    // Check if this column is needed anymore
    if ((newColumnInfo.counter == 0) && (newColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(filterInfo.filterColumn);
    }
}

// Updates the column info map
void JoinTreeNode::estimateInfoAfterFilterEqual(FilterInfo& filterInfo) {
    ColumnInfo oldColumnInfo = this->usedColumnInfos[filterInfo.filterColumn];

    // Update the info of the column on which the filter is applied
    ColumnInfo newColumnInfo;

    newColumnInfo.min      = filterInfo.constant;
    newColumnInfo.max      = filterInfo.constant;
    newColumnInfo.distinct = 1;
    newColumnInfo.size     = oldColumnInfo.size / oldColumnInfo.distinct;
    newColumnInfo.n        = 1;
    newColumnInfo.spread   = 1;
    newColumnInfo.counter  = oldColumnInfo.counter - 1;
    newColumnInfo.isSelectionColumn = oldColumnInfo.isSelectionColumn;
    newColumnInfo.updateSize        = oldColumnInfo.updateSize;

    this->usedColumnInfos[filterInfo.filterColumn] = newColumnInfo;

    // Update the info of the other columns
    for (columnInfoMap::iterator it=this->usedColumnInfos.begin(); it != this->usedColumnInfos.end(); it++) {
        if (!(it->first == filterInfo.filterColumn)) {
            if(it->second.distinct == 0) it->second.distinct = 1;
            double base      = 1 - (((double) newColumnInfo.size) / ((double) oldColumnInfo.size));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            if(it->second.distinct == 0) it->second.distinct = 1;
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
        }
    }

    // Check if this column is needed anymore
    if ((newColumnInfo.counter == 0) && (newColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(filterInfo.filterColumn);
    }
}

// Estimates the info of a node's column
// after a join predicate is applied to its children
void JoinTreeNode::estimateInfoAfterJoin(PredicateInfo& predicateInfo) {
    // Get the column info of the columns to be joined from the children
    ColumnInfo leftColumnInfo = this->left->usedColumnInfos[predicateInfo.left];
    ColumnInfo rightColumnInfo = this->right->usedColumnInfos[predicateInfo.right];

    //----------------
    /*
    fprintf(stderr, "Before predicate %d.%d=%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId,
        predicateInfo.right.binding, predicateInfo.right.colId);
    fprintf(stderr, "%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId);
    leftColumnInfo->print();
    fprintf(stderr, "%d.%d\n", predicateInfo.right.binding, predicateInfo.right.colId);
    rightColumnInfo->print();
    */
    //----------------

    // Save the current min and max in case they change
    uint64_t oldLeftMin  = leftColumnInfo.min;
    uint64_t oldLeftMax  = leftColumnInfo.max;
    uint64_t oldRightMin = rightColumnInfo.min;
    uint64_t oldRightMax = rightColumnInfo.max;

    // If the domains are not the same apply a custom filter
    if ((oldLeftMin != oldRightMin) || (oldLeftMax != oldRightMax)) {
        // First apply the right filters to create the same domain on both columns
        uint64_t oldDistinct    = leftColumnInfo.distinct;
        leftColumnInfo.min      = max(leftColumnInfo.min, rightColumnInfo.min);
        leftColumnInfo.max      = min(leftColumnInfo.max, rightColumnInfo.max);
        leftColumnInfo.distinct = (uint64_t) (((double) (leftColumnInfo.max - leftColumnInfo.min)) / leftColumnInfo.spread);
        if (leftColumnInfo.distinct == 0) leftColumnInfo.distinct = 1;
        leftColumnInfo.size     = leftColumnInfo.distinct * (leftColumnInfo.size / oldDistinct + 1);
        leftColumnInfo.n        = leftColumnInfo.max - leftColumnInfo.min + 1;
        leftColumnInfo.spread   = ((double) leftColumnInfo.n) / ((double) leftColumnInfo.distinct);

        oldDistinct              = rightColumnInfo.distinct;
        rightColumnInfo.min      = leftColumnInfo.min;
        rightColumnInfo.max      = leftColumnInfo.max;
        rightColumnInfo.distinct = (uint64_t) (((double) (rightColumnInfo.max - rightColumnInfo.min)) / rightColumnInfo.spread);
        if (rightColumnInfo.distinct == 0) rightColumnInfo.distinct = 1;
        rightColumnInfo.size     = rightColumnInfo.distinct * (rightColumnInfo.size / oldDistinct);
        rightColumnInfo.n        = rightColumnInfo.max - rightColumnInfo.min + 1;
        rightColumnInfo.spread   = ((double) rightColumnInfo.n) / ((double) rightColumnInfo.distinct);

        //---------------
        /*
        fprintf(stderr, "AFTER APPLYING A CUSTOM FILTER\n");
        fprintf(stderr, "%d.%d\n", predicateInfo.left.binding, predicateInfo.left.colId);
        leftColumnInfo->print();
        fprintf(stderr, "%d.%d\n", predicateInfo.right.binding, predicateInfo.right.colId);
        rightColumnInfo->print();
        */
        //---------------
    }

    //this->estimateInfoAfterIndependentJoin(predicateInfo, leftColumnInfo, rightColumnInfo);
    this->estimateInfoAfterLeftDependentJoin(predicateInfo, leftColumnInfo, rightColumnInfo);

/*
    if ((oldLeftMin >= oldRightMin) && (oldLeftMax <= rightColumnInfo->max)) {
        this->estimateInfoAfterLeftDependentJoin(predicateInfo, leftColumnInfo, rightColumnInfo);
    }
    //else if ((rightColumnInfo->min >= leftColumnInfo->min) && (rightColumnInfo->max <= leftColumnInfo->max)) {
    //    this->estimateInfoAfterRightDependentJoin(predicateInfo);
    //}
    else {
        this->estimateInfoAfterIndependentJoin(predicateInfo);
    }
*/
}

// Updates the column info map
ColumnInfo JoinTreeNode::estimateInfoAfterLeftDependentJoin(PredicateInfo& predicateInfo, ColumnInfo oldLeftColumnInfo, ColumnInfo oldRightColumnInfo) {
    ColumnInfo newLeftColumnInfo, newRightColumnInfo;

    // Estimate the new info of the columns to be joined
    newLeftColumnInfo.min      = oldLeftColumnInfo.min;
    newLeftColumnInfo.max      = oldLeftColumnInfo.max;
    newLeftColumnInfo.size     = oldLeftColumnInfo.size * (oldRightColumnInfo.size / oldRightColumnInfo.distinct);
    newLeftColumnInfo.distinct = oldLeftColumnInfo.distinct;
    if(newLeftColumnInfo.distinct == 0) newLeftColumnInfo.distinct = 1;
    newLeftColumnInfo.n        = newLeftColumnInfo.max - newLeftColumnInfo.min + 1;
    newLeftColumnInfo.spread   = ((double) newLeftColumnInfo.n) / ((double) newLeftColumnInfo.distinct);
    newLeftColumnInfo.counter  = oldLeftColumnInfo.counter - 1;
    newLeftColumnInfo.isSelectionColumn = oldLeftColumnInfo.isSelectionColumn;
    newLeftColumnInfo.updateSize        = true;

    newRightColumnInfo.min      = oldRightColumnInfo.min;
    newRightColumnInfo.max      = oldRightColumnInfo.max;
    newRightColumnInfo.size     = oldLeftColumnInfo.size * (oldRightColumnInfo.size / oldRightColumnInfo.distinct);
    newRightColumnInfo.distinct = oldLeftColumnInfo.distinct;
    if(newRightColumnInfo.distinct == 0) newRightColumnInfo.distinct = 1;
    newRightColumnInfo.n        = newRightColumnInfo.max - newRightColumnInfo.min + 1;
    newRightColumnInfo.spread   = ((double) newRightColumnInfo.n) / ((double) newRightColumnInfo.distinct);
    newRightColumnInfo.counter  = oldRightColumnInfo.counter - 1;
    newRightColumnInfo.isSelectionColumn = oldRightColumnInfo.isSelectionColumn;
    newRightColumnInfo.updateSize        = true;

    this->usedColumnInfos[predicateInfo.left] = newLeftColumnInfo;
    this->usedColumnInfos[predicateInfo.right] = newRightColumnInfo;

    // Update the info of the other columns
    // in the same relation as the left predicate
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.left)) && (it->first.binding == predicateInfo.left.binding)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            ColumnInfo newRandomColumnInfo = it->second;

            double base      = 1 - (((double) newLeftColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) newRandomColumnInfo.size) / ((double) newRandomColumnInfo.distinct);
            double tempValue = pow(base, exponent);

            newRandomColumnInfo.size         = newLeftColumnInfo.size;
            //newRandomColumnInfo.distinct     = ceil(newRandomColumnInfo.distinct * (1 - tempValue));
            newRandomColumnInfo.distinct     = min(newRandomColumnInfo.size, it->second.distinct);
            if (newRandomColumnInfo.distinct == 0) newRandomColumnInfo.distinct = 1;
            newRandomColumnInfo.spread       = ((double) newRandomColumnInfo.n) / ((double) newRandomColumnInfo.distinct);
            newRandomColumnInfo.updateSize   = true;
            this->usedColumnInfos[it->first] = newRandomColumnInfo;
        }
    }

    // Update the info of the other columns
    // in the same relation as the right predicate
    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.right)) && (it->first.binding == predicateInfo.right.binding)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            ColumnInfo newRandomColumnInfo = it->second;

            double base      = 1 - (((double) newRightColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) newRandomColumnInfo.size) / ((double) newRandomColumnInfo.distinct);
            double tempValue = pow(base, exponent);

            newRandomColumnInfo.size         = newRightColumnInfo.size;
            //newRandomColumnInfo.distinct     = ceil(newRandomColumnInfo.distinct * (1 - tempValue));
            newRandomColumnInfo.distinct     = min(newRandomColumnInfo.size, it->second.distinct);
            if (newRandomColumnInfo.distinct == 0) newRandomColumnInfo.distinct = 1;
            newRandomColumnInfo.spread       = ((double) newRandomColumnInfo.n) / ((double) newRandomColumnInfo.distinct);
            newRandomColumnInfo.updateSize   = true;
            this->usedColumnInfos[it->first] = newRandomColumnInfo;
        }
    }

    // Update the needed column sizes
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->second.updateSize == true) {
            it->second.size = newLeftColumnInfo.size;;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->second.updateSize == true) {
            it->second.size = newRightColumnInfo.size;;
        }
    }

    // Copy every other remaining column
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.left.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.right.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Check if the left column is needed anymore
    if ((newLeftColumnInfo.counter == 0) && (newLeftColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.left);
    }

    // Check if the right column is needed anymore
    if ((newRightColumnInfo.counter == 0) && (newRightColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.right);
    }
}

// Updates the column info map
ColumnInfo JoinTreeNode::estimateInfoAfterRightDependentJoin(PredicateInfo& predicateInfo) {
    ColumnInfo newLeftColumnInfo, newRightColumnInfo;

    // Get the info of the columns to be joined
    ColumnInfo oldLeftColumnInfo = this->left->usedColumnInfos[predicateInfo.left];
    ColumnInfo oldRightColumnInfo = this->right->usedColumnInfos[predicateInfo.right];

    // Estimate the new info of the columns to be joined
    newLeftColumnInfo.min      = oldLeftColumnInfo.min;
    newLeftColumnInfo.max      = oldLeftColumnInfo.max;
    newLeftColumnInfo.size     = oldRightColumnInfo.size * (oldLeftColumnInfo.size / oldLeftColumnInfo.distinct);
    newLeftColumnInfo.distinct = oldRightColumnInfo.distinct;
    if(newLeftColumnInfo.distinct == 0) newLeftColumnInfo.distinct = 1;
    newLeftColumnInfo.n        = newLeftColumnInfo.max - newLeftColumnInfo.min + 1;
    newLeftColumnInfo.spread   = ((double) newLeftColumnInfo.n) / ((double) newLeftColumnInfo.distinct);
    newLeftColumnInfo.counter  = oldLeftColumnInfo.counter - 1;
    newLeftColumnInfo.isSelectionColumn = oldLeftColumnInfo.isSelectionColumn;

    newRightColumnInfo.min      = oldRightColumnInfo.min;
    newRightColumnInfo.max      = oldRightColumnInfo.max;
    newRightColumnInfo.size     = oldRightColumnInfo.size * (oldLeftColumnInfo.size / oldLeftColumnInfo.distinct);
    newRightColumnInfo.distinct = oldRightColumnInfo.distinct;
    if(newRightColumnInfo.distinct == 0) newRightColumnInfo.distinct = 1;
    newRightColumnInfo.n        = newRightColumnInfo.max - newRightColumnInfo.min + 1;
    newRightColumnInfo.spread   = ((double) newRightColumnInfo.n) / ((double) newRightColumnInfo.distinct);
    newRightColumnInfo.counter  = oldRightColumnInfo.counter - 1;
    newRightColumnInfo.isSelectionColumn = oldRightColumnInfo.isSelectionColumn;

    this->usedColumnInfos[predicateInfo.left] = newLeftColumnInfo;
    this->usedColumnInfos[predicateInfo.right] = newRightColumnInfo;

    // Update the info of the other columns
    // in the same relation as the left predicate
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.left)) && (it->first.binding == predicateInfo.left.binding)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            double base      = 1 - (((double) newLeftColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newLeftColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            if(it->second.distinct == 0) it->second.distinct = 1;
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Update the info of the other columns
    // in the same relation as the right predicate
    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.right)) && (it->first.binding == predicateInfo.right.binding)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            double base      = 1 - (((double) newRightColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) it->second.size) / ((double) it->second.distinct);
            double tempValue = pow(base, exponent);

            it->second.size     = newRightColumnInfo.size;
            it->second.distinct = ceil(it->second.distinct * (1 - tempValue));
            if(it->second.distinct == 0) it->second.distinct = 1;
            it->second.spread   = ((double) it->second.n) / ((double) it->second.distinct);
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Copy every other remaining column
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.left.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.right.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Check if the left column is needed anymore
    if ((newLeftColumnInfo.counter == 0) && (newLeftColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.left);
    }

    // Check if the right column is needed anymore
    if ((newRightColumnInfo.counter == 0) && (newRightColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.right);
    }
}

// Updates the column info map
ColumnInfo JoinTreeNode::estimateInfoAfterIndependentJoin(PredicateInfo& predicateInfo, ColumnInfo oldLeftColumnInfo, ColumnInfo oldRightColumnInfo) {
    ColumnInfo newLeftColumnInfo, newRightColumnInfo;

    // Get the info of the columns to be joined
    // ColumnInfo oldLeftColumnInfo = this->left->usedColumnInfos[predicateInfo.left];
    // ColumnInfo oldRightColumnInfo = this->right->usedColumnInfos[predicateInfo.right];

    // Estimate the new info of the columns to be joined
    newLeftColumnInfo.min      = oldLeftColumnInfo.min;
    newLeftColumnInfo.max      = oldLeftColumnInfo.max;
    newLeftColumnInfo.n        = newLeftColumnInfo.max - newLeftColumnInfo.min + 1;
    newLeftColumnInfo.size     = (oldLeftColumnInfo.size * oldRightColumnInfo.size) / newLeftColumnInfo.n;
    newLeftColumnInfo.distinct = oldRightColumnInfo.distinct;
    if(newLeftColumnInfo.distinct == 0) newLeftColumnInfo.distinct = 1;
    newLeftColumnInfo.spread   = ((double) newLeftColumnInfo.n) / ((double) newLeftColumnInfo.distinct);
    newLeftColumnInfo.counter  = oldLeftColumnInfo.counter - 1;
    newLeftColumnInfo.isSelectionColumn = oldLeftColumnInfo.isSelectionColumn;
    newLeftColumnInfo.updateSize        = true;

    newRightColumnInfo.min      = oldRightColumnInfo.min;
    newRightColumnInfo.max      = oldRightColumnInfo.max;
    newRightColumnInfo.n        = newRightColumnInfo.max - newRightColumnInfo.min + 1;
    newRightColumnInfo.size     = (oldLeftColumnInfo.size * oldRightColumnInfo.size) / newRightColumnInfo.n;
    newRightColumnInfo.distinct = oldRightColumnInfo.distinct;
    if(newRightColumnInfo.distinct == 0) newRightColumnInfo.distinct = 1;
    newRightColumnInfo.spread   = ((double) newRightColumnInfo.n) / ((double) newRightColumnInfo.distinct);
    newRightColumnInfo.counter  = oldRightColumnInfo.counter - 1;
    newRightColumnInfo.isSelectionColumn = oldRightColumnInfo.isSelectionColumn;
    newRightColumnInfo.updateSize        = true;

    this->usedColumnInfos[predicateInfo.left] = newLeftColumnInfo;
    this->usedColumnInfos[predicateInfo.right] = newRightColumnInfo;

    // Update the info of the other columns
    // in the same relation as the left predicate
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.left)) && (it->first.binding == predicateInfo.left.binding)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            ColumnInfo newRandomColumnInfo = it->second;

            double base      = 1 - (((double) newLeftColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) newRandomColumnInfo.size) / ((double) newRandomColumnInfo.distinct);
            double tempValue = pow(base, exponent);

            newRandomColumnInfo.size         = newLeftColumnInfo.size;
            newRandomColumnInfo.distinct     = ceil(newRandomColumnInfo.distinct * (1 - tempValue));
            //newRandomColumnInfo.distinct     = min(newRandomColumnInfo.size, it->second.distinct);
            if (newRandomColumnInfo.distinct == 0) newRandomColumnInfo.distinct = 1;
            newRandomColumnInfo.spread       = ((double) newRandomColumnInfo.n) / ((double) newRandomColumnInfo.distinct);
            newRandomColumnInfo.updateSize   = true;
            this->usedColumnInfos[it->first] = newRandomColumnInfo;
        }
    }

    // Update the info of the other columns
    // in the same relation as the right predicate
    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if ((!(it->first == predicateInfo.right)) && (it->first.binding == predicateInfo.right.binding)) {
            if (it->second.distinct == 0) it->second.distinct = 1;
            ColumnInfo newRandomColumnInfo = it->second;

            double base      = 1 - (((double) newRightColumnInfo.size) / (((double) oldLeftColumnInfo.size) * ((double) oldRightColumnInfo.size)));
            double exponent  = ((double) newRandomColumnInfo.size) / ((double) newRandomColumnInfo.distinct);
            double tempValue = pow(base, exponent);

            newRandomColumnInfo.size         = newRightColumnInfo.size;
            newRandomColumnInfo.distinct     = ceil(newRandomColumnInfo.distinct * (1 - tempValue));
            //newRandomColumnInfo.distinct     = min(newRandomColumnInfo.size, it->second.distinct);
            if (newRandomColumnInfo.distinct == 0) newRandomColumnInfo.distinct = 1;
            newRandomColumnInfo.spread       = ((double) newRandomColumnInfo.n) / ((double) newRandomColumnInfo.distinct);
            newRandomColumnInfo.updateSize   = true;
            this->usedColumnInfos[it->first] = newRandomColumnInfo;
        }
    }

    // Update the needed column sizes
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->second.updateSize == true) {
            it->second.size = newLeftColumnInfo.size;;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->second.updateSize == true) {
            it->second.size = newRightColumnInfo.size;;
        }
    }

    // Copy every other remaining column
    for (columnInfoMap::iterator it=this->left->usedColumnInfos.begin(); it != this->left->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.left.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    for (columnInfoMap::iterator it=this->right->usedColumnInfos.begin(); it != this->right->usedColumnInfos.end(); it++) {
        if (it->first.binding != predicateInfo.right.binding) {
            this->usedColumnInfos[it->first] = it->second;
        }
    }

    // Check if the left column is needed anymore
    if ((newLeftColumnInfo.counter == 0) && (newLeftColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.left);
    }

    // Check if the right column is needed anymore
    if ((newRightColumnInfo.counter == 0) && (newRightColumnInfo.isSelectionColumn == false)) {
        this->usedColumnInfos.erase(predicateInfo.right);
    }
}

// Construct a JoinTree from a set of relations
JoinTree* JoinTree::build(QueryInfo& queryInfo, ColumnInfo** columnInfos) {
    // Maps every possible set of relations to its respective best plan tree
    unordered_map< vector<bool>, JoinTree* > BestTree;
    int relationsCount = queryInfo.relationIds.size();

    // Initialise the BestTree structure with nodes
    // for every single relation in the input

    for (int i = 0; i < relationsCount; i++) {
        // Allocate memory
        JoinTree* joinTreePtr = new JoinTree();
        JoinTreeNode* joinTreeNodePtr = new JoinTreeNode();

        // Initialise JoinTreeNode
        joinTreeNodePtr->nodeId = i; // The binding of the relation
        joinTreeNodePtr->treeCost = 0;
        joinTreeNodePtr->left = NULL;
        joinTreeNodePtr->right = NULL;
        joinTreeNodePtr->parent = NULL;
        joinTreeNodePtr->predicatePtr = NULL;

        // Save the initial info of all the columns to be used in the query
        unsigned relationId, columnId;

        for (auto predicate : queryInfo.predicates) {
            if (predicate.left.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(predicate.left) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = predicate.left.relId;
                    columnId = predicate.left.colId;
                    joinTreeNodePtr->usedColumnInfos[predicate.left] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[predicate.left].counter++;
            }
            else if (predicate.right.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(predicate.right) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = predicate.right.relId;
                    columnId = predicate.right.colId;
                    joinTreeNodePtr->usedColumnInfos[predicate.right] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[predicate.right].counter++;
            }
        }

        for (auto filter : queryInfo.filters) {
            if (filter.filterColumn.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(filter.filterColumn) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = filter.filterColumn.relId;
                    columnId = filter.filterColumn.colId;
                    joinTreeNodePtr->usedColumnInfos[filter.filterColumn] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[filter.filterColumn].counter++;
            }
        }

        for (auto selection : queryInfo.selections) {
            if (selection.binding == i) {
                // If the column is not already in the map insert it's info
                if (joinTreeNodePtr->usedColumnInfos.find(selection) == joinTreeNodePtr->usedColumnInfos.end()) {
                    relationId = selection.relId;
                    columnId = selection.colId;
                    joinTreeNodePtr->usedColumnInfos[selection] = columnInfos[relationId][columnId];
                }

                joinTreeNodePtr->usedColumnInfos[selection].counter++;
                joinTreeNodePtr->usedColumnInfos[selection].isSelectionColumn = true;
            }
        }

        // Initialise JoinTree
        joinTreePtr->root = joinTreeNodePtr;

        // Insert into the BestTree
        vector<bool> relationToVector(relationsCount, false);
        relationToVector[i] = true;
        BestTree[relationToVector] = joinTreePtr;
    }

    // Maps all sets of a certain size to their size
    // Sets are represented as vector<bool>
    map<int, set<vector<bool> > > powerSetMap;

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

    // Apply all the filters first
    for (int i=0; i < queryInfo.filters.size(); i++) {
        // Update the tree (containing a single node)
        // of the relation whose column will be filtered
        vector<bool> relationToVector(relationsCount, false);
        relationToVector[queryInfo.filters[i].filterColumn.binding] = true;
        BestTree[relationToVector]->root->filterPtrs.push_back(&(queryInfo.filters[i]));

        // Update the column info
        BestTree[relationToVector]->root->estimateInfoAfterFilter(queryInfo.filters[i]);
    }

    // Dynamic programming algorithm
    for (int i = 1; i < relationsCount; i++) {
        for (auto s : powerSetMap[i]) {
            for (int j = 0; j < relationsCount; j++) {
                // If j is not in the set
                if (s[j] == false) {
                    // Check if there is a corresponding predicate
                    for (auto predicate : queryInfo.predicates) {
                        // If the right relation is found on the right hand side of a predicate
                        if (predicate.right.binding == j) {
                            for (int n = 0; n < relationsCount; n++) {
                                // If a relation from the set is found on the left hand side of the same predicate
                                if ((s[n] == true) && (predicate.left.binding == n)) {
                                    // If no predicate exists for the relations in the set
                                    // a tree has not been created
                                    if (BestTree[s] == NULL) continue;

                                    // Create the bit vector representation of the relation j
                                    vector<bool> relationToVector(relationsCount, false);
                                    relationToVector[j] = true;

                                    // Merge the two trees
                                    JoinTree* currTree = CreateJoinTree(BestTree[s], BestTree[relationToVector], predicate);

                                    // Save the new merged tree
                                    vector<bool> s1 = s;
                                    s1[j] = true;

                                    if ((BestTree[s1] == NULL) || (BestTree[s1]->getCost() > currTree->getCost())) {
                                        BestTree[s1] = currTree;
                                    }
                                }
                            }
                        }
                        // If the right relation is found on the right hand side of a predicate
                        else if (predicate.left.binding == j) {
                            for (int n = 0; n < relationsCount; n++) {
                                // If a relation from the set is found on the left hand side of the same predicate
                                if ((s[n] == true) && (predicate.right.binding == n)) {
                                    // If no predicate exists for the relations in the set
                                    // a tree has not been created
                                    if (BestTree[s] == NULL) continue;

                                    // Create the bit vector representation of the relation j
                                    vector<bool> relationToVector(relationsCount, false);
                                    relationToVector[j] = true;

                                    // Merge the two trees
                                    PredicateInfo temp = predicate;
                                    temp.left = predicate.right;
                                    temp.right = predicate.left;

                                    JoinTree* currTree = CreateJoinTree(BestTree[s], BestTree[relationToVector], temp);

                                    // Save the new merged tree
                                    vector<bool> s1 = s;
                                    s1[j] = true;

                                    if ((BestTree[s1] == NULL) || (BestTree[s1]->getCost() > currTree->getCost())) {
                                        BestTree[s1] = currTree;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Create a set of the predicates
    set<PredicateInfo*> predicatesSet;
    for (int i = 0; i < queryInfo.predicates.size(); i++) {
        predicatesSet.insert(&(queryInfo.predicates[i]));
    }

    // Set the predicates for every node
    vector<bool> rootToVector(relationsCount, true);
    JoinTreeNode* joinTreeNodePtr = BestTree[rootToVector]->root;
    set<int> joinedNodes; // Keep the bindings of the already joined nodes

    // Go to the leftmost join
    while (joinTreeNodePtr->left->nodeId == -1) {
        JoinTreeNode* temp = joinTreeNodePtr;
        if (joinTreeNodePtr->right != NULL) {
            joinTreeNodePtr->right->parent = temp;
        }
        joinTreeNodePtr = joinTreeNodePtr->left;
        joinTreeNodePtr->parent = temp;
    }

    // Restore the initial column infos of the left child
    // for (columnInfoMap::iterator it=joinTreeNodePtr->left->usedColumnInfos.begin(); it != joinTreeNodePtr->left->usedColumnInfos.end(); it++) {
    //     it->second = columnInfos[it->first.relId][it->first.colId];
    // }

    // Go bottom-up and save the corresponding predicates
    joinedNodes.insert(joinTreeNodePtr->left->nodeId);

    while (1) {
        bool predicateFound = false;
        for (auto predicate : predicatesSet) {
            // If the right relation is found on the right hand side of a predicate
            if (predicate->right.binding == joinTreeNodePtr->right->nodeId) {
                for (auto n : joinedNodes) {
                    if (predicate->left.binding == n) {
                        joinTreeNodePtr->predicatePtr = predicate;
                        predicatesSet.erase(predicate);
                        joinedNodes.insert(joinTreeNodePtr->right->nodeId);
                        predicateFound = true;
                        break;
                    }
                }
            }
            else if (predicate->left.binding == joinTreeNodePtr->right->nodeId) {
                for (auto n : joinedNodes) {
                    if (predicate->right.binding == n) {
                        SelectInfo tempLeft = predicate->left;
                        predicate->left = predicate->right;
                        predicate->right = tempLeft;

                        joinTreeNodePtr->predicatePtr = predicate;
                        predicatesSet.erase(predicate);
                        joinedNodes.insert(joinTreeNodePtr->right->nodeId);
                        predicateFound = true;
                        break;
                    }
                }
            }

            if (predicateFound == true) break;
        }

        // Restore the initial column infos of the right child
        // for (columnInfoMap::iterator it=joinTreeNodePtr->right->usedColumnInfos.begin(); it != joinTreeNodePtr->right->usedColumnInfos.end(); it++) {
        //     it->second = columnInfos[it->first.relId][it->first.colId];
        // }

        // Go to parent
        if (joinTreeNodePtr->parent != NULL) {
            joinTreeNodePtr = joinTreeNodePtr->parent;
        }
        else {
            break;
        }
    }

    // Return the optimal tree
    while (predicatesSet.size() != 0) {
        // Merge the self joins with the root
        BestTree[rootToVector] = AddFilterJoin(BestTree[rootToVector], *(predicatesSet.begin()));
        predicatesSet.erase(predicatesSet.begin());
    }

    return BestTree[rootToVector];
}

// Merges two join trees
JoinTree* JoinTree::CreateJoinTree(JoinTree* leftTree, JoinTree* rightTree, PredicateInfo& predicateInfo) {
    // Allocate memory for the new tree
    JoinTree* joinTreePtr = new JoinTree();
    JoinTreeNode* joinTreeNodePtr = new JoinTreeNode();

    // Initialise the new JoinTreeNode
    joinTreeNodePtr->nodeId = -1; // This is an intermediate node
    joinTreeNodePtr->treeCost = 0;
    joinTreeNodePtr->left = leftTree->root;
    joinTreeNodePtr->right = rightTree->root;
    joinTreeNodePtr->parent = NULL;
    joinTreeNodePtr->predicatePtr = NULL;

    // Assign a cost to this node
    joinTreeNodePtr->cost(predicateInfo);

    // Estimate the new info of the merged columns
    joinTreeNodePtr->estimateInfoAfterJoin(predicateInfo);

    // Initialise the new JoinTree
    joinTreePtr->root = joinTreeNodePtr;

    return joinTreePtr;
}

// Merges the final optimal tree with a filter join predicate
JoinTree* JoinTree::AddFilterJoin(JoinTree* leftTree, PredicateInfo* predicateInfo) {
    // Allocate memory for the new tree
    JoinTree* joinTreePtr = new JoinTree();
    JoinTreeNode* joinTreeNodePtr = new JoinTreeNode();

    // Initialise the new JoinTreeNode
    joinTreeNodePtr->nodeId = -1; // This is an intermediate node
    joinTreeNodePtr->left = leftTree->root;
    joinTreeNodePtr->right = NULL;
    joinTreeNodePtr->parent = NULL;
    joinTreeNodePtr->treeCost = joinTreeNodePtr->left->treeCost + joinTreeNodePtr->left->usedColumnInfos[predicateInfo->left].size;
    joinTreeNodePtr->predicatePtr = predicateInfo;
    joinTreeNodePtr->usedColumnInfos = joinTreeNodePtr->left->usedColumnInfos;

    ColumnInfo *leftColumnInfo, *rightColumnInfo;
    leftColumnInfo = &(joinTreeNodePtr->usedColumnInfos[predicateInfo->left]);
    rightColumnInfo = &(joinTreeNodePtr->usedColumnInfos[predicateInfo->right]);

    // Check if the left column is needed anymore
    leftColumnInfo->counter--;
    if ((leftColumnInfo->counter == 0) && (leftColumnInfo->isSelectionColumn == false)) {
        joinTreeNodePtr->usedColumnInfos.erase(predicateInfo->left);
    }

    // Check if the right column is needed anymore
    rightColumnInfo->counter--;
    if ((rightColumnInfo->counter == 0) && (rightColumnInfo->isSelectionColumn == false)) {
        joinTreeNodePtr->usedColumnInfos.erase(predicateInfo->right);
    }

    // Initialise the new JoinTree
    joinTreePtr->root = joinTreeNodePtr;

    // Update the parent pointers of the merged trees
    leftTree->root->parent = joinTreePtr->root;
    return joinTreePtr;
}

//#define prints

// Execute the plan described by a JoinTree
table_t* JoinTreeNode::execute(JoinTreeNode* joinTreeNodePtr, Joiner& joiner, QueryInfo& queryInfo, string& result_str, bool* stop) {
    JoinTreeNode *left  = joinTreeNodePtr->left;
    JoinTreeNode *right = joinTreeNodePtr->right;
    table_t *table_l;
    table_t *table_r;
    table_t *res;
    int leafs = 0;

    // Leaf node containing a single relation
    if (left == NULL && right == NULL) {
        res = joiner.CreateTableTFromId(queryInfo.relationIds[joinTreeNodePtr->nodeId], joinTreeNodePtr->nodeId);

        // Apply the filters
        for (auto filter : joinTreeNodePtr->filterPtrs) {
            joiner.AddColumnToTableT(filter->filterColumn, res);
            joiner.Select(*filter, res, &(joinTreeNodePtr->usedColumnInfos[filter->filterColumn]));
        }
        return res;
    }

    // Go left
    table_l = joinTreeNodePtr->execute(left, joiner, queryInfo, result_str, stop);

    // Return without executing the rest of the tree
    if (*stop == true) {
        return NULL;
    }

    // This is an intermediate node (join)
    if (right != NULL) {
        table_r = joinTreeNodePtr->execute(right, joiner, queryInfo, result_str, stop);

        // If either column has no tuples stop the execution
        if ((table_l->tups_num == 0) || (table_r->tups_num == 0)) {
            *stop = true;

            for (size_t i = 0; i < queryInfo.selections.size(); i++) {
                result_str += "NULL";

                // Create the write check sum
                if (i != queryInfo.selections.size() - 1) {
                    result_str +=  " ";
                }
            }

            return NULL;
        }

        // Calculate leafs
        if (left->nodeId != -1 && left->filterPtrs.size() == 0) {
            leafs = 1;
        }
        if (right->nodeId != -1 && right->filterPtrs.size() == 0) {
            leafs |= 2;
        }

        if (joinTreeNodePtr->parent == NULL) {
            res = joiner.join(table_l, table_r, *joinTreeNodePtr->predicatePtr,
                             joinTreeNodePtr->usedColumnInfos, true,
                             queryInfo.selections, leafs, result_str);
        }
        else {
            res = joiner.join(table_l, table_r, *joinTreeNodePtr->predicatePtr,
                             joinTreeNodePtr->usedColumnInfos, false,
                             queryInfo.selections, leafs, result_str);
        }
        return res;
    }
    else {
        if (joinTreeNodePtr->parent == NULL) {
            res = joiner.SelfJoinCheckSumOnTheFly(table_l, joinTreeNodePtr->predicatePtr, joinTreeNodePtr->usedColumnInfos
                                                  ,queryInfo.selections, result_str);
        }
        else {
            res = joiner.SelfJoin(table_l, joinTreeNodePtr->predicatePtr, joinTreeNodePtr->usedColumnInfos);
        }
        return res;

    }
}

table_t* JoinTreeNode::execute_t64(JoinTreeNode* joinTreeNodePtr, Joiner& joiner, QueryInfo& queryInfo, string& result_str, bool* stop) {
    JoinTreeNode *left  = joinTreeNodePtr->left;
    JoinTreeNode *right = joinTreeNodePtr->right;
    table_t *table_l;
    table_t *table_r;
    table_t *res;
    int leafs = 0;

    // Leaf node containing a single relation
    if (left == NULL && right == NULL) {
        res = joiner.CreateTableTFromId(queryInfo.relationIds[joinTreeNodePtr->nodeId], joinTreeNodePtr->nodeId);

        // Apply the filters
        for (auto filter : joinTreeNodePtr->filterPtrs) {
            joiner.AddColumnToTableT(filter->filterColumn, res);
            joiner.Select(*filter, res, &(joinTreeNodePtr->usedColumnInfos[filter->filterColumn]));
        }
        return res;
    }

    // Go left
    table_l = joinTreeNodePtr->execute_t64(left, joiner, queryInfo, result_str, stop);

    // Return without executing the rest of the tree
    if (*stop == true) {
        return NULL;
    }

    // This is an intermediate node (join)
    if (right != NULL) {
        table_r = joinTreeNodePtr->execute_t64(right, joiner, queryInfo, result_str, stop);

        // If either column has no tuples stop the execution
        if ((table_l->tups_num == 0) || (table_r->tups_num == 0)) {
            *stop = true;

            for (size_t i = 0; i < queryInfo.selections.size(); i++) {
                result_str += "NULL";

                // Create the write check sum
                if (i != queryInfo.selections.size() - 1) {
                    result_str +=  " ";
                }
            }

            return NULL;
        }
        // Calculate leafs
        if (left->nodeId != -1 && left->filterPtrs.size() == 0) {
            leafs = 1;
        }
        if (right->nodeId != -1 && right->filterPtrs.size() == 0) {
            leafs |= 2;
        }

        if (joinTreeNodePtr->parent == NULL) {
            res = joiner.join_t64(table_l, table_r, *joinTreeNodePtr->predicatePtr,
                             joinTreeNodePtr->usedColumnInfos, true,
                             queryInfo.selections, leafs, result_str);
        }
        else {
            res = joiner.join_t64(table_l, table_r, *joinTreeNodePtr->predicatePtr,
                             joinTreeNodePtr->usedColumnInfos, false,
                             queryInfo.selections, leafs, result_str);
        }
        return res;
    }
    else {
        if (joinTreeNodePtr->parent == NULL) {
            res = joiner.SelfJoinCheckSumOnTheFly(table_l, joinTreeNodePtr->predicatePtr, joinTreeNodePtr->usedColumnInfos
                                                  ,queryInfo.selections, result_str);
        }
        else {
            res = joiner.SelfJoin(table_l, joinTreeNodePtr->predicatePtr, joinTreeNodePtr->usedColumnInfos);
        }
        return res;

    }
}


// Estimate the cost of a JoinTreeNode
void JoinTreeNode::cost(PredicateInfo& predicateInfo) {
    this->treeCost = this->left->treeCost +
                     this->left->usedColumnInfos[predicateInfo.left].size;
}

// Returns the cost of a given JoinTree
uint64_t JoinTree::getCost() {
    return this->root->treeCost;
}

void JoinTreeNode::print(JoinTreeNode* joinTreeNodePtr) {
    if (joinTreeNodePtr == NULL) {
        return;
    }

    fprintf(stderr, "\n");
    int depth = 0;

    while (joinTreeNodePtr->nodeId == -1) {
        for (int i=0; i < depth; i++) fprintf(stderr, "    ");
        //fprintf(stderr, "In node with predicate: ");
        fprintf(stderr,"%d.%d=%d.%d\n", joinTreeNodePtr->predicatePtr->left.binding,
            joinTreeNodePtr->predicatePtr->left.colId, joinTreeNodePtr->predicatePtr->right.binding,
            joinTreeNodePtr->predicatePtr->right.colId);

        if (joinTreeNodePtr->right != NULL) {
            // for (int i=0; i < depth; i++) fprintf(stderr,"    ");
            // fprintf(stderr, "Right child has id = %d\n", joinTreeNodePtr->right->nodeId);
            // if (joinTreeNodePtr->right->filterPtrs.size() > 0) {
            //     for (auto filter : joinTreeNodePtr->right->filterPtrs) {
            //         for (int i=0; i < depth; i++) fprintf(stderr,"    ");
            //         fprintf(stderr, "Right child has filters = ");
            //         fprintf(stderr,"%d.%d %c %ld\n", filter->filterColumn.binding, filter->filterColumn.colId,
            //             filter->comparison, filter->constant);
            //     }
            // }
            // else {
            //     for (int i=0; i < depth; i++) fprintf(stderr,"    ");
            //     fprintf(stderr, "Right child has no filter\n");
            // }
        }
        else {
            // for (int i=0; i < depth; i++) fprintf(stderr,"    ");
            // fprintf(stderr, "Node has no right child so this is a filter join\n");
        }

        // for (int i=0; i < depth; i++) fprintf(stderr,"    ");
        // fprintf(stderr, "Left child has id = %d\n", joinTreeNodePtr->left->nodeId);
        // if (joinTreeNodePtr->left->filterPtrs.size() > 0) {
        //     for (auto filter : joinTreeNodePtr->left->filterPtrs) {
        //         for (int i=0; i < depth; i++) fprintf(stderr,"    ");
        //         fprintf(stderr, "Left child has filters = ");
        //         fprintf(stderr,"%d.%d %c %ld\n", filter->filterColumn.binding, filter->filterColumn.colId,
        //             filter->comparison, filter->constant);
        //     }
        // }
        // else {
        //     for (int i=0; i < depth; i++) fprintf(stderr,"    ");
        //     fprintf(stderr, "Left child has no filter\n");
        // }

        joinTreeNodePtr = joinTreeNodePtr->left;
        depth++;
    }
    fprintf(stderr, "\n");
}

// Fills the columnInfo matrix with the data of every column
void QueryPlan::fillColumnInfo(Joiner& joiner, JobScheduler & j1, JobScheduler & j2, bool & switch_64) {
    Relation* relation;
    int relationsCount = joiner.getRelationsCount();
    size_t allColumns = 0; // Number of all columns of all relations
    size_t relationColumns; // Number of columns of a single relation

    // Get the number of all columns
    for (int rel = 0; rel < relationsCount; rel++) {
        allColumns += joiner.getRelation(rel).columns.size();
    }

    // Create a vector of pointers to all columns
    vector<uint64_t*> columnPtrs(allColumns);
    vector<uint64_t> columnTuples(allColumns);
    vector<ColumnInfo> columnInfosVector(allColumns);
    int index = 0;

    for (int rel = 0; rel < relationsCount; rel++) {
        // Get the number of columns of this relation
        relation = &(joiner.getRelation(rel));
        relationColumns = relation->columns.size();

        for (int col = 0; col < relationColumns; col++) {
            columnPtrs[index] = relation->columns[col];
            columnTuples[index] = relation->size;
            index++;
        }
    }

    // Get the statistics of every column
    StatisticsThreadArgs* args = (StatisticsThreadArgs*) malloc(allColumns * sizeof(StatisticsThreadArgs));
    for (int i = 0; i < allColumns; i++) {
        args[i].columnPtr    = columnPtrs[i];
        args[i].columnTuples = columnTuples[i];
        args[i].columnInfo   = &columnInfosVector[i];
        if (i % 2 == 0)
            j1.Schedule(new StatisticsJob(&args[i]));
        else
            j2.Schedule(new StatisticsJob(&args[i]));
    }

    // Wait for the threads to finish
    j1.Barrier();
    j2.Barrier();

    // Now we have to transfrom the vector of columnInfo to a 2 dimensional matrix
    index = 0;

    // Allocate memory for every relation
    columnInfos = (ColumnInfo**) malloc(relationsCount * sizeof(ColumnInfo*));

    // For every relation allocate memory for its columns
    for (int rel = 0; rel < relationsCount; rel++) {
        // Get the number of columns
        relationColumns = joiner.getRelation(rel).columns.size();
        columnInfos[rel] = (ColumnInfo*) malloc(relationColumns * sizeof(ColumnInfo));

        // Get the info of every column
        for (int col = 0; col < relationColumns; col++) {
            columnInfos[rel][col] = columnInfosVector[index];
            index++;            

            // Check the max value of a column
            if (columnInfos[rel][col].max >= numeric_limits<uint32_t>::max()) {
                switch_64 = true;
            }
        }
    }
}


// JoinTreeNode destructor
void JoinTreeNode::destroy() {
    JoinTreeNode* joinTreeNodePtr = this;

    // Got to leftmost join
    while (joinTreeNodePtr->nodeId == -1) {
        joinTreeNodePtr = joinTreeNodePtr->left;
    }

    // Go up and free
    while (joinTreeNodePtr->parent != NULL) {
        delete(joinTreeNodePtr->left);
        delete(joinTreeNodePtr->right);
        joinTreeNodePtr = joinTreeNodePtr->parent;
    }

    // Root node
    delete(joinTreeNodePtr->left);
    delete(joinTreeNodePtr->right);
    delete(joinTreeNodePtr);
}

// JoinTree destructor
void JoinTree::destroy() {
    this->root->destroy();
}

// QueryPlan destructor
void QueryPlan::destroy(Joiner& joiner) {
    int relationsCount = joiner.getRelationsCount();

    for (int rel = 0; rel < relationsCount; rel++) {
        free(columnInfos[rel]);
    }
    free(columnInfos);
}

/* -----------------------CACHING--------------------------------*/
relation_t * CreateRowRelationT(uint64_t * column, unsigned size, JobScheduler &js1, JobScheduler &js2) {

    /* Create the relation_t */
    relation_t * new_relation = gen_rel(size);

    // Get the range for the threds chinking
    size_t range = THREAD_NUM_1CPU + THREAD_NUM_2CPU;

    /* Initialize relation */
    struct noninterRel_arg a[range];
    for (size_t i = 0; i < range; i++) {
        a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
        a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
        a[i].values = column;
        a[i].tups = new_relation->tuples;
        if (i % 2 == 0 )
            js1.Schedule(new JobCreateNonInterRel(a[i]));
        else
            js2.Schedule(new JobCreateNonInterRel(a[i]));

    }
    js1.Barrier();
    js2.Barrier();
    return new_relation;
}


// cache(sleep time) the partitions of 0,1 columns for all relations
void QueryPlan::Pre_Caching(Joiner& joiner,JobScheduler & j1, JobScheduler & j2, struct timeval & start) {
    uint64_t *col0_ptr;
    relation_t *r0;
    int nthreads = THREAD_NUM_1CPU;
    size_t relationColumns; // Number of columns of a single relation

    /* Now iterate through all relation's 0,1 cols */
    /* Create select info for col 0,1 and pass it to selection */
    /* then partion and store in our map */
    Relation* relation;
    int relationsCount = joiner.getRelationsCount();

    double time_margin = 1000;
    double time_elapsed;
    double safe_time = 0;

    struct timeval end;
    gettimeofday(&end, NULL);
    time_elapsed = (double) ((end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0);

    for (int rel = 0; (rel < relationsCount) && (time_elapsed * 1000 < time_margin - safe_time * 1000); rel++) {
        /* create a new entry for the map */
        /* get the relation */
        relation = &(joiner.getRelation(rel));

        /* Get the number of the relation */
        relationColumns = relation->columns.size();

        for (size_t col = 0; col < relationColumns; col++) {
            struct timeval relTimeStart;   // time one relation
            gettimeofday(&relTimeStart, NULL);

            Cacheinf c;
            c.R  = (cached_t *) calloc(nthreads, sizeof(cached_t));
            c.S  = NULL;
            relation_t * r0 = CreateRowRelationT(relation->columns[col], relation->size, j1, j2);

            cache_partition_0(r0, nthreads, c, j1, j2);

            SelectInfo col_sel_inf(rel, 0, col);
            Selection  col_sel(col_sel_inf);
            idxcache[col_sel] = c.R;

            // Time one creation with strict policy
            struct timeval relTimeEnd;   // time one relation
            gettimeofday(&relTimeEnd, NULL);
            double tmp_safe_time = (double) ((relTimeEnd.tv_sec - relTimeStart.tv_sec) + (relTimeEnd.tv_usec - relTimeStart.tv_usec) / 1000000.0);
            if (safe_time < tmp_safe_time)
                safe_time = tmp_safe_time;

            gettimeofday(&end, NULL);
            time_elapsed = (double) ((end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0);
            if (time_elapsed * 1000 > time_margin - safe_time * 1000) break;
        }
    }

    gettimeofday(&end, NULL);
}
