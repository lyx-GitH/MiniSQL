//
// Created by 刘宇轩 on 2022/5/27.
//

#ifndef MINISQL_INTERVALMERGE_H
#define MINISQL_INTERVALMERGE_H

#include <unordered_set>
#include "common/rowid.h"

void set_or(std::unordered_set<RowId>& a, std::unordered_set<RowId>& b){
  if(b.size() > a.size())
    std::swap(a, b);
  for(auto rid: b) {
    if(a.find(rid) == a.end())
      a.insert(rid);
  }
}

void set_and(std::unordered_set<RowId>& a, std::unordered_set<RowId> &b) {
  if(a.size() > b.size())
    std::swap(a, b);
  std::unordered_set<RowId> ans;
  for(auto rid : a)
    if(b.count(rid))
      ans.insert(rid);
  swap(ans, a);
}

#endif  // MINISQL_INTERVALMERGE_H
