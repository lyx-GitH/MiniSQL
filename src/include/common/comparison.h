//
// Created by 刘宇轩 on 2022/5/28.
//

#ifndef MINISQL_COMPARISON_H
#define MINISQL_COMPARISON_H

#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include "record/field.h"

typedef std::function<bool(const Field &, const Field &)> comp_func;
std::string sEq = "=";
std::string sNeq = "<>";
std::string sGt = ">";
std::string sLt = "<";
std::string sGte = ">=";
std::string sLte = "<=";
std::string sIsNull = "is";
std::string sIsNNull = "not";

const comp_func eq = [](const Field &a, const Field &b) -> bool { return a.CompareEquals(b) == kTrue; };

const comp_func neq = [](const Field &a, const Field &b) -> bool { return a.CompareNotEquals(b) == kTrue; };

const comp_func lt = [](const Field &a, const Field &b) -> bool { return a.CompareLessThan(b) == kTrue; };

const comp_func lte = [](const Field &a, const Field &b) -> bool { return a.CompareLessThanEquals(b) == kTrue; };

const comp_func gt = [](const Field &a, const Field &b) -> bool { return a.CompareGreaterThan(b) == kTrue; };

const comp_func gte = [](const Field &a, const Field &b) -> bool { return a.CompareGreaterThanEquals(b) == kTrue; };

const comp_func isNull = [](const Field &a, const Field &b) -> bool { return a.IsNull(); };

const comp_func notNull = [](const Field &a, const Field &b) -> bool { return !a.IsNull(); };

const static std::unordered_map<std::string, comp_func> comparisons{
    {sGt, gt}, {sLt, lt}, {sLte, lte}, {sGte, gte}, {sEq, eq}, {sNeq, neq}, {sIsNNull, notNull}, {sIsNull, isNull}};
extern const std::unordered_map<std::string, comp_func> comparisons;

struct index_com_args {
  bool left;
  bool key_included;
};

const static std::unordered_map<std::string, index_com_args> idx_comps{
    {sGt, {false, false}}, {sLt, {true, false}}, {sGte, {false, true}}, {sLte, {true, true}}};
extern const std::unordered_map<std::string, index_com_args> idx_comps;

#endif  // MINISQL_COMPARISON_H
