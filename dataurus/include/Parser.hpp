#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include "Relation.hpp"

struct SelectInfo {
    RelationId relId; // Relation id
    unsigned binding; // Binding for the relation
    unsigned colId; // Column id

    // The constructor
    SelectInfo(RelationId relId,unsigned b,unsigned colId) : relId(relId), binding(b), colId(colId){};
    SelectInfo(void){};

    // copy conustroctor
    SelectInfo(const SelectInfo& si) : relId(si.relId), binding(si.binding), colId(si.colId){};

    // Equality operator
    bool operator==(const SelectInfo& o) const;

    // Dump text format
    std::string dumpText();

    // Dump SQL
    std::string dumpSQL(bool addSUM=false);

    static const char delimiter=' '; // The delimiter used in our text format
    constexpr static const char delimiterSQL[]=", "; // The delimiter used in SQL

};

struct FilterInfo {
    enum Comparison : char { Less='<', Greater='>', Equal='=' };

    SelectInfo filterColumn; // Filter Column
    uint64_t constant; // Constant
    Comparison comparison; // Comparison type

    // Dump SQL
    std::string dumpSQL();

    // The constructor
    FilterInfo(SelectInfo filterColumn,uint64_t constant,Comparison comparison) : filterColumn(filterColumn), constant(constant), comparison(comparison) {};
    FilterInfo(void){};
    // copy constructor
        FilterInfo (const FilterInfo& fi): filterColumn(fi.filterColumn), constant(fi.constant), comparison(fi.comparison) {};

    // Dump text format
    std::string dumpText();

    static const char delimiter='&'; // The delimiter used in our text format
    constexpr static const char delimiterSQL[]=" and "; // The delimiter used in SQL
};

static const std::vector<FilterInfo::Comparison> comparisonTypes { FilterInfo::Comparison::Less, FilterInfo::Comparison::Greater, FilterInfo::Comparison::Equal};

struct PredicateInfo {
    SelectInfo left; // Left
    SelectInfo right; // Right

    // The constructor
    PredicateInfo(SelectInfo left, SelectInfo right) : left(left), right(right){};
    PredicateInfo(void){};
    // copy constructor
    PredicateInfo(const PredicateInfo& pi) : left(pi.left), right(pi.right){};

    // Dump text format
    std::string dumpText();

    // Dump SQL
    std::string dumpSQL();

    static const char delimiter='&'; // The delimiter used in our text format
    constexpr static const char delimiterSQL[]=" and "; // The delimiter used in SQL
};

class QueryInfo {
    public:
    std::vector<RelationId> relationIds; // The relation ids
    std::vector<PredicateInfo> predicates; // The predicates
    std::vector<FilterInfo> filters; // The filters
    std::vector<SelectInfo> selections; // The selections

    // Reset query info
    void clear();

    private:
    // Parse a single predicate
    void parsePredicate(std::string& rawPredicate);

    // Resolve bindings of relation ids
    void resolveRelationIds();

    public:

    // Parse relation ids <r1> <r2> ...
    void parseRelationIds(std::string& rawRelations);

    // Parse predicates r1.a=r2.b&r1.b=r3.c...
    void parsePredicates(std::string& rawPredicates);

    // Parse selections r1.a r1.b r3.c...
    void parseSelections(std::string& rawSelections);

    // Parse selections [RELATIONS]|[PREDICATES]|[SELECTS]
    void parseQuery(std::string& rawQuery);

    // Dump text format
    std::string dumpText();

    // Dump SQL
    std::string dumpSQL();

    // The empty constructor
    QueryInfo() {}

    // The constructor that parses a query
    QueryInfo(std::string rawQuery);
};


class Selection {
    public:
    RelationId relId; // Relation id
    unsigned colId; // Column id

    //Selection(const RelationId rel, const unsigned col): relId(rel), colId(col){};
    Selection(const SelectInfo& si) : relId(si.relId), colId(si.colId){};
};


bool operator<(const PredicateInfo& lhs, const PredicateInfo& rhs);
bool operator<(const FilterInfo& lhs, const FilterInfo& rhs);
bool operator<(const SelectInfo& lhs, const SelectInfo& rhs);
bool operator<(const Selection& lhs, const Selection& rhs);
