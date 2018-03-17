#include "radix.h"
#include <stdio.h>

int main() {
  int num_tuples = 10000000;
  relation_t r1;
  r1.tuples = malloc(num_tuples*sizeof(tuple_t));
  relation_t r2;
  r2.tuples = malloc(num_tuples*sizeof(tuple_t));

  for (int i = 0; i < num_tuples; ++i) {
    r1.num_tuples = num_tuples;
    r1.tuples[i].key = i;
    r1.tuples[i].payload = i;
    
    r2.num_tuples = num_tuples;
    r2.tuples[i].key = i;
    r2.tuples[i].payload = i;
  }
    

  result_t *res = RJ(&r1, &r2, 1);
  printf("%ld\n", res->totalresults);
}


