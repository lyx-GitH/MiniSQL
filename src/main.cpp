#include <chrono>
#include <cstdio>
#include "common/macros.h"
#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

#define SYNTAX_OUT

extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);
  printf("minisql > ");
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  getchar();      // remove enter
}

int main(int argc, char **argv) {
  InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];
  // execute engine
  ExecuteEngine engine;
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  [[maybe_unused]] uint32_t syntax_tree_id = 0;

  std::chrono::time_point<std::chrono::system_clock> start, end;
  dberr_t res;

  SAY_HELLO;

  while (true) {
    // read from buffer
    InputCommand(cmd, buf_size);
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("\033[1;31m%s\033[0m\n", MinisqlParserGetErrorMessage());
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
#endif
    }
#ifdef SYNTAX_OUT
    SyntaxTreePrinter t(MinisqlGetParserRootNode());
    t.PrintTree(std::cout);
#endif
    ExecuteContext context;

    /*====================== Execution Start =======================*/
    start = std::chrono::system_clock::now();
    res = engine.Execute(MinisqlGetParserRootNode(), &context);
    end = std::chrono::system_clock::now();
    /*====================== Execution End ========================*/

    if (res == DB_SUCCESS)
      printf("\033[1;32m[Succeeded] \033[0m in %llu ms\n",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    else
      printf("\033[1;31m[Failed] \033[0m in %llu ms, code: %d\n",
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), res);

    sleep(1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }
  }
  return 0;
}