#include <fstream>
#include "gtest/gtest.h"
#include "Relation.hpp"
#include "Utils.hpp"
//---------------------------------------------------------------------------
static void ASSERT_RELATION_EQ(Relation& r1,Relation& r2)
{
  ASSERT_EQ(r1.size,r2.size);
  ASSERT_EQ(r1.columns.size(),r2.columns.size());
  for (unsigned i=0;i<r1.columns.size();++i) {
    ASSERT_EQ(memcmp(r1.columns[i],r2.columns[i],r1.size*sizeof(uint64_t)),0);
  }
}
//---------------------------------------------------------------------------
TEST(Relation,LoadAndStore) {
  Relation r1=Utils::createRelation(1000,5);

  r1.storeRelation("r1");
  // Load it back from disk
  Relation r2("r1");

  ASSERT_RELATION_EQ(r1,r2);
}
//---------------------------------------------------------------------------
TEST(Relation,EmptyRelation) {
  Relation r1=Utils::createRelation(0,0);

  r1.storeRelation("r1");

  // Load it back from disk
  Relation r2("r1");

  ASSERT_RELATION_EQ(r1,r2);
}
//---------------------------------------------------------------------------
TEST(Relation,StoreCsv) {
  Relation r1=Utils::createRelation(1000,2);

  r1.storeRelationCSV("r1");

  std::ifstream infile("r1.tbl");
  std::string line;

  unsigned i=0;
  while (std::getline(infile, line)) {
    auto col=std::to_string(i)+"|";
    auto expected=col+col; // 2 columns
    ASSERT_EQ(expected,line);
    i++;
  }
}
//---------------------------------------------------------------------------
TEST(Relation,CreateSQL) {
  Relation r1=Utils::createRelation(1,5);

  r1.dumpSQL("r1",3);

  std::ifstream infile("r1.sql");
  std::string line;

  ASSERT_TRUE(std::getline(infile, line));
  ASSERT_EQ("CREATE TABLE r3 (c0 bigint,c1 bigint,c2 bigint,c3 bigint,c4 bigint);",line);
  ASSERT_TRUE(std::getline(infile, line));
  ASSERT_EQ("copy r3 from 'r3.tbl' delimiter '|';",line);
  ASSERT_FALSE(std::getline(infile, line));
}
//---------------------------------------------------------------------------
