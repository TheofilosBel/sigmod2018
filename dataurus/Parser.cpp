#include <cassert>
#include <iostream>
#include <utility>
#include <sstream>
#include "Parser.hpp"

using namespace std;

// Split a line into numbers
static void splitString(string& line,vector<unsigned>& result,const char delimiter) {
    stringstream ss(line);
    string token;
    while (getline(ss,token,delimiter)) {
        result.push_back(stoul(token));
    }
}

// Parse a line into strings
static void splitString(string& line,vector<string>& result,const char delimiter) {
    stringstream ss(line);
    string token;
    while (getline(ss,token,delimiter)) {
        result.push_back(token);
    }
}

// Split a line into predicate strings
static void splitPredicates(string& line,vector<string>& result) {
    // Determine predicate type
    for (auto cT : comparisonTypes) {
        if (line.find(cT)!=string::npos) {
            splitString(line,result,cT);
            break;
        }
    }
}

// Parse a string of relation ids
void QueryInfo::parseRelationIds(string& rawRelations) {
    splitString(rawRelations,relationIds,' ');
}

static SelectInfo parseRelColPair(string& raw) {
    vector<unsigned> ids;
    splitString(raw,ids,'.');
    return SelectInfo(0,ids[0],ids[1]);
}

inline static bool isConstant(string& raw) { return raw.find('.')==string::npos; }

// Parse a single predicate: join "r1Id.col1Id=r2Id.col2Id" or "r1Id.col1Id=constant" filter
void QueryInfo::parsePredicate(string& rawPredicate) {
    vector<string> relCols;
    splitPredicates(rawPredicate,relCols);
    assert(relCols.size()==2);
    assert(!isConstant(relCols[0])&&"left side of a predicate is always a SelectInfo");
    auto leftSelect=parseRelColPair(relCols[0]);
    if (isConstant(relCols[1])) {
        uint64_t constant=stoul(relCols[1]);
        char compType=rawPredicate[relCols[0].size()];
        filters.emplace_back(leftSelect,constant,FilterInfo::Comparison(compType));
    }
    else {
        predicates.emplace_back(leftSelect,parseRelColPair(relCols[1]));
    }
}

// Parse predicates
void QueryInfo::parsePredicates(string& text) {
    vector<string> predicateStrings;
    splitString(text,predicateStrings,'&');
    for (auto& rawPredicate : predicateStrings) {
        parsePredicate(rawPredicate);
    }
}

// Parse selections
void QueryInfo::parseSelections(string& rawSelections) {
    vector<string> selectionStrings;
    splitString(rawSelections,selectionStrings,' ');
    for (auto& rawSelect : selectionStrings) {
        selections.emplace_back(parseRelColPair(rawSelect));
    }
}

// Resolve relation id
static void resolveIds(vector<unsigned>& relationIds,SelectInfo& selectInfo) {
    selectInfo.relId=relationIds[selectInfo.binding];
}

// Resolve relation ids
void QueryInfo::resolveRelationIds() {
    // Selections
    for (auto& sInfo : selections) {
        resolveIds(relationIds,sInfo);
    }

    // Predicates
    for (auto& pInfo : predicates) {
        resolveIds(relationIds,pInfo.left);
        resolveIds(relationIds,pInfo.right);
    }

    // Filters
    for (auto& fInfo : filters) {
        resolveIds(relationIds,fInfo.filterColumn);
    }
}

// Parse query [RELATIONS]|[PREDICATES]|[SELECTS]
void QueryInfo::parseQuery(string& rawQuery) {
    clear();
    vector<string> queryParts;
    splitString(rawQuery,queryParts,'|');
    assert(queryParts.size()==3);
    parseRelationIds(queryParts[0]);
    parsePredicates(queryParts[1]);
    parseSelections(queryParts[2]);
    resolveRelationIds();
}

// Reset query info
void QueryInfo::clear() {
    relationIds.clear();
    predicates.clear();
    filters.clear();
    selections.clear();
}

// Wraps relation id into quotes to be a SQL compliant string
static string wrapRelationName(uint64_t id) {
    return "\""+to_string(id)+"\"";
}

// Appends a selection info to the stream
string SelectInfo::dumpSQL(bool addSUM) {
    auto innerPart=wrapRelationName(binding)+".c"+to_string(colId);
    return addSUM?"SUM("+innerPart+")":innerPart;
}

// Dump text format
string SelectInfo::dumpText() {
    return to_string(binding)+"."+to_string(colId);
}

// Dump text format
string FilterInfo::dumpText() {
    return filterColumn.dumpText()+static_cast<char>(comparison)+to_string(constant);
}

// Dump text format
string FilterInfo::dumpSQL() {
    return filterColumn.dumpSQL()+static_cast<char>(comparison)+to_string(constant);
}

// Dump text format
string PredicateInfo::dumpText() {
    return left.dumpText()+'='+right.dumpText();
}

// Dump text format
string PredicateInfo::dumpSQL() {
    return left.dumpSQL()+'='+right.dumpSQL();
}

template <typename T>
static void dumpPart(stringstream& ss,vector<T> elements) {
    for (unsigned i=0;i<elements.size();++i) {
        ss << elements[i].dumpText();
        if (i<elements.size()-1)
            ss << T::delimiter;
    }
}
/*
template <typename T>
static void dumpPartSQL(stringstream& ss,vector<T> elements) {
    for (unsigned i=0;i<elements.size();++i) {
        ss << elements[i].dumpSQL();
        if (i<elements.size()-1)
            ss << T::delimiterSQL;
    }
}
*/
// Dump text format
string QueryInfo::dumpText() {
    stringstream text;
    // Relations
    for (unsigned i=0;i<relationIds.size();++i) {
        text << relationIds[i];
        if (i<relationIds.size()-1)
            text << " ";
    }
    text << "|";

    dumpPart(text,predicates);
    if (predicates.size()&&filters.size())
        text << PredicateInfo::delimiter;
    dumpPart(text,filters);
    text << "|";
    dumpPart(text,selections);

    return text.str();
}

// Dump SQL
/*
string QueryInfo::dumpSQL() {
    stringstream sql;
    sql << "SELECT ";
    for (unsigned i=0;i<selections.size();++i) {
        sql << selections[i].dumpSQL(true);
        if (i<selections.size()-1)
            sql << ", ";
    }

    sql << " FROM ";
    for (unsigned i=0;i<relationIds.size();++i) {
        sql << "r" << relationIds[i] << " " << wrapRelationName(i);
        if (i<relationIds.size()-1)
            sql << ", ";
    }

    sql << " WHERE ";
    dumpPartSQL(sql,predicates);
    if (predicates.size()&&filters.size())
        sql << " and ";
    dumpPartSQL(sql,filters);

    sql << ";";

    return sql.str();
}
*/
QueryInfo::QueryInfo(string rawQuery) { parseQuery(rawQuery); }

bool SelectInfo::operator==(const SelectInfo& o) const {
   return o.relId == relId && o.binding == binding && o.colId == colId;
}

bool operator<(const PredicateInfo& lhs, const PredicateInfo& rhs) {
    if  (lhs.left < rhs.left)
        return true;
    else if (rhs.left < lhs.left)
        return false;
    else
        return lhs.right < rhs.right;
}


bool operator<(const FilterInfo& lhs, const FilterInfo& rhs) {
    if (lhs.filterColumn < rhs.filterColumn)
        return true;
    else if (rhs.filterColumn < lhs.filterColumn)
        return false;
    else if (lhs.comparison < rhs.comparison)
        return true;
    else if (rhs.comparison < lhs.comparison)
        return false;
    else
        return lhs.constant < rhs.constant;
}

bool operator<(const SelectInfo& lhs, const SelectInfo& rhs) {
    if (lhs.binding < rhs.binding)
        return true;
    else if (rhs.binding < lhs.binding)
        return false;
    else
        return lhs.colId < rhs.colId;
}

bool operator<(const Selection& lhs, const Selection& rhs) {
    if (lhs.relId < rhs.relId)
        return true;
    else if (rhs.relId < lhs.relId)
        return false;
    else
        return lhs.colId < rhs.colId;
}
