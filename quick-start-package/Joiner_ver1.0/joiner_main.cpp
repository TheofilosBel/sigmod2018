#include "iostream"
#include "joiner.hpp"


int main(int argc, char const *argv[]) {
	/* Create a joiner and a column*/
	
	/*
	joiner_t *joiner = new joiner_t;
	column_t *column = new column_t;
	*/

	/* Initilize the joiner / column */
	/*
	joiner->row_ids     = new int* [2];
	joiner->row_ids[0]     = new int[3]; joiner->row_ids[1]     = new int[3];
	joiner->row_ids[0][0] = 1 ; joiner->row_ids[0][1] = 2; joiner->row_ids[0][2] = 3;
	joiner->row_ids[1][0] = 3 ; joiner->row_ids[1][1] = 4; joiner->row_ids[1][2] = 5;
	joiner->sizes       = new int[2];
	joiner->sizes[0] = 3; joiner->sizes[1] = 3;
	column->values      = new int[10];
	for (int i = 0; i < 10; i++) {
		column->values[i] = i;
	}
	column->table_id    = 1;
	column->size        = 10;
	PrintColumn(column);
	*/

	/* Call the construct function */
	//column_t *constructed_columnt = construct(column, joiner);

	/* Print the column */
	//PrintColumn(constructed_columnt);



	/* here testing the join function with the updated .hpp */
	column_t *r  = new column_t;
	r->values = new int [5];
	r->size = 5;
	r->table_id = 0;
	for (int i = 0; i < 5; i++) {
		r->values[i] = i*2;
	}

	column_t *s = new column_t;
	s->values = new int[12];
	s->size = 12;
	s->table_id = 1;
	for (int i = 0; i < 12; i++) {
		s->values[i] = i*20;
	}

	joiner_t *joiner = new joiner_t;
	joiner->row_ids = new std::vector<int>*[2];
	joiner->row_ids[0] = new std::vector<int>;
	joiner->row_ids[1] = new std::vector<int>;

	joiner_t *join_result = join(r, s, joiner);

	std::cout<<"Print rowids:"<<std::endl;
	for (int i = 0; i < 2; i++) {
		std::cout<<"**Print rowids of relation->"<<i<<std::endl;
		for(auto k : *(join_result->row_ids[i]))
			std::cout<<k<<"\n";
		std::cout<<"End of relation->"<<i<<"**"<<"\n\n";
	}
	return 0;
}
