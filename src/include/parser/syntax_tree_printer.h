//
// Created by Chengjun Ying on 2022/4/5.
//
#ifndef MINISQL_SYNTAX_TREE_PRINTER_H
#define MINISQL_SYNTAX_TREE_PRINTER_H

#include <iostream>
#include <string>

extern "C" {
#include "parser/syntax_tree.h"
};

class SyntaxTreePrinter {
public:
  explicit SyntaxTreePrinter(pSyntaxNode root) : root_(root) {};

  void PrintTree(std::ostream &out);

private:
  void PrintTreeLow(pSyntaxNode node, std::ostream &out);

private:
  pSyntaxNode root_;
};

#endif //MINISQL_SYNTAX_TREE_PRINTER_H
