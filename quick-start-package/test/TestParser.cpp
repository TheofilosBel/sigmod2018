#include "gtest/gtest.h"
#include "Parser.hpp"
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
TEST(Parser,ParseRelations) {
  QueryInfo i;
  auto& relationIds=i.relationIds;
  string rel("0 1");
  i.parseRelationIds(rel);

  ASSERT_EQ(relationIds.size(),2u);
  ASSERT_EQ(relationIds[0],0u);
  ASSERT_EQ(relationIds[1],1u);
}
//---------------------------------------------------------------------------
static void assertPredicatEqual(PredicateInfo& pInfo,unsigned leftRel,unsigned leftCol,unsigned rightRel,unsigned rightCol)
{
  ASSERT_EQ(pInfo.left.relId,leftRel);
  ASSERT_EQ(pInfo.left.colId,leftCol);
  ASSERT_EQ(pInfo.right.relId,rightRel);
  ASSERT_EQ(pInfo.right.colId,rightCol);
}
//---------------------------------------------------------------------------
static void assertPredicateBindingEqual(PredicateInfo& pInfo,unsigned leftBind,unsigned leftCol,unsigned rightBind,unsigned rightCol)
{
  ASSERT_EQ(pInfo.left.binding,leftBind);
  ASSERT_EQ(pInfo.left.colId,leftCol);
  ASSERT_EQ(pInfo.right.binding,rightBind);
  ASSERT_EQ(pInfo.right.colId,rightCol);
}
//---------------------------------------------------------------------------
static void assertSelectEqual(SelectInfo& sInfo,unsigned rel,unsigned col)
{
  ASSERT_EQ(sInfo.relId,rel);
  ASSERT_EQ(sInfo.colId,col);
}
//---------------------------------------------------------------------------
static void assertSelectBindingEqual(SelectInfo& sInfo,unsigned binding,unsigned col)
{
  ASSERT_EQ(sInfo.binding,binding);
  ASSERT_EQ(sInfo.colId,col);
}
//---------------------------------------------------------------------------
static void assertFilterBindingEqual(FilterInfo& fInfo,unsigned binding,unsigned col,uint64_t constant,FilterInfo::Comparison comparison)
{
  assertSelectBindingEqual(fInfo.filterColumn,binding,col);
  ASSERT_EQ(fInfo.constant,constant);
  ASSERT_EQ(fInfo.comparison,comparison);
}
//---------------------------------------------------------------------------
static void assertFilterEqual(FilterInfo& fInfo,unsigned rel,unsigned col,uint64_t constant,FilterInfo::Comparison comparison)
{
  assertSelectEqual(fInfo.filterColumn,rel,col);
  ASSERT_EQ(fInfo.constant,constant);
  ASSERT_EQ(fInfo.comparison,comparison);
}
//---------------------------------------------------------------------------
TEST(Parser,ParsePredicates) {
  string preds("0.2=1.3&2.2=3.3");
  QueryInfo i;
  auto& predicates=i.predicates; auto& filters=i.filters;
  i.parsePredicates(preds);

  ASSERT_EQ(predicates.size(),2u);
  assertPredicateBindingEqual(predicates[0],0,2,1,3);
  assertPredicateBindingEqual(predicates[1],2,2,3,3);

  i.clear();
  string emptyPreds("");
  i.parsePredicates(emptyPreds);
  ASSERT_EQ(predicates.size(),0u);

  i.clear();
  string onlyFilters("0.1=111&1.2<222&1.1>333");
  i.parsePredicates(onlyFilters);
  ASSERT_EQ(predicates.size(),0u);
  ASSERT_EQ(filters.size(),3u);
  assertFilterBindingEqual(filters[0],0,1,111,FilterInfo::Comparison::Equal);
  assertFilterBindingEqual(filters[1],1,2,222,FilterInfo::Comparison::Less);
  assertFilterBindingEqual(filters[2],1,1,333,FilterInfo::Comparison::Greater);

  i.clear();
  string mixedPredicatesAndFilters("0.1=111&1.2=2.1&1.2<333");
  i.parsePredicates(mixedPredicatesAndFilters);
  ASSERT_EQ(predicates.size(),1u);
  assertPredicateBindingEqual(predicates[0],1,2,2,1);
  ASSERT_EQ(filters.size(),2u);
  assertFilterBindingEqual(filters[0],0,1,111,FilterInfo::Comparison::Equal);
  assertFilterBindingEqual(filters[1],1,2,333,FilterInfo::Comparison::Less);
}
//---------------------------------------------------------------------------
TEST(Parser,ParseSelections) {
  string rawSelections("0.1 0.2 1.2 4.4");
  QueryInfo i;
  auto& selections=i.selections;
  i.parseSelections(rawSelections);

  ASSERT_EQ(selections.size(),4u);
  assertSelectBindingEqual(selections[0],0,1);
  assertSelectBindingEqual(selections[1],0,2);
  assertSelectBindingEqual(selections[2],1,2);
  assertSelectBindingEqual(selections[3],4,4);


  selections.clear();
  string emptySelect("");
  i.parseSelections(emptySelect);
  ASSERT_EQ(selections.size(),0u);
}
//---------------------------------------------------------------------------
TEST(Parser,ParseQuery) {
  string rawQuery("0 2 4|0.1=1.1&0.0=2.1&1.0=2.0&1.0>3|0.1 1.4 2.2");
  QueryInfo i(rawQuery);

  ASSERT_EQ(i.relationIds.size(),3u);
  ASSERT_EQ(i.relationIds[0],0u);
  ASSERT_EQ(i.relationIds[1],2u);
  ASSERT_EQ(i.relationIds[2],4u);

  ASSERT_EQ(i.predicates.size(),3u);
  assertPredicatEqual(i.predicates[0],0,1,2,1);
  assertPredicatEqual(i.predicates[1],0,0,4,1);
  assertPredicatEqual(i.predicates[2],2,0,4,0);

  assertFilterEqual(i.filters[0],2,0,3,FilterInfo::Comparison::Greater);

  ASSERT_EQ(i.selections.size(),3u);
  assertSelectEqual(i.selections[0],0,1);
  assertSelectEqual(i.selections[1],2,4);
  assertSelectEqual(i.selections[2],4,2);
}
//---------------------------------------------------------------------------
TEST(Parser,DumpSQL) {
  string rawQuery("0 2|0.1=1.1&0.0=1.0&0.1=5|0.1 1.4");
  QueryInfo i(rawQuery);

  auto sql=i.dumpSQL();
  ASSERT_EQ("SELECT SUM(\"0\".c1), SUM(\"1\".c4) FROM r0 \"0\", r2 \"1\" WHERE \"0\".c1=\"1\".c1 and \"0\".c0=\"1\".c0 and \"0\".c1=5;",sql);
}
//---------------------------------------------------------------------------
TEST(Parser,DumpText) {
  string rawQuery("0 2|0.1=1.1&0.0=1.0&1.2=3|0.1 1.4");
  QueryInfo i(rawQuery);

  ASSERT_EQ(i.dumpText(),rawQuery);
}
