//
// Created by 刘宇轩 on 2022/6/1.
//

#ifndef MINISQL_FORMAT_PRINT_H
#define MINISQL_FORMAT_PRINT_H

#include "config.h"

using std::cout;
using std::endl;
using std::string;
using std::vector;
using rows = vector<string>;

uint32_t max(uint32_t a, uint32_t b) { return a > b ? a : b; }

void output(const uint32_t len, const std::string &str) {
  auto indent = len - str.length();
  std::string blanks(indent, ' ');
  cout << str << blanks;
}

void format_print(vector<rows> &grid, bool with_head = false) {
  if (grid.empty()) return;

  vector<uint32_t> max_lengths(grid[0].size(), 1);

  // get max_lengths
  if(with_head)
    std::sort(grid.begin()+1, grid.end(), [](const rows &a, const rows &b) { return a[0] < b[0]; });
  else std::sort(grid.begin(), grid.end(), [](const rows &a, const rows &b) { return a[0] < b[0]; });
  for (uint32_t i = 0; i < grid.size(); i++) {
    for (uint32_t j = 0; j < grid[i].size(); j++) {
      max_lengths[j] = max(max_lengths[j], grid[i][j].length());
    }
  }

  std::string line = "+";
  for (auto l : max_lengths) {
    line += "-" + string(l, '-') + "-+";
  }

  cout << line << endl;
  for (uint32_t i = 0; i < grid.size(); i++) {
    cout << "⎢";
    for (uint32_t j = 0; j < grid[0].size(); j++) {
      cout<<" ";
      output(max_lengths[j], grid[i][j]);
      cout << " ⎢";
    }
    cout << endl;
    cout << line<<endl;
  }

  std::cout <<"("<< grid.size()<<" rows)" << std::endl;
}

#endif  // MINISQL_FORMAT_PRINT_H
