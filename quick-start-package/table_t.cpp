#include <stdlib>
#include "table_t.hpp"
#include "Parser.hpp"
#include "Joiner.hpp"

column_t *CreateColumn(SelectInfo& sel_info,  Joiner& joiner) {

	/* Get relation */
	Relation& rel = joiner.getRelation(sel_info.relId);

	/* Create and initialize the column */
	column_t * column = (column_t *) malloc(sizeof(column_t));
	column->size      = rel.size;
	column->values    = rel.columns[sel_info.colId];
	column->binding   = rel.binding;

	return column;  //TODO delete it when done
}