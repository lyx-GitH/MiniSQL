#ifndef MINISQL_TRANSACTION_H
#define MINISQL_TRANSACTION_H

#include <unordered_map>

#include <string>

/**
 * Transaction tracks information related to a transaction.
 *
 * Implemented by student self
*/
class Transaction {
  private:
  std::unordered_map<std::string, std::string> Contends;
//  std::list<char*> records;

 public:
  const char *get(const std::string& key) {
    if(Contends.count(key) == 0)
      return nullptr;
    else return Contends[key].c_str();
  }

  bool set(const std::string& key, const std::string& value) {
    if(Contends.count(key) == 0){
      Contends[key] = value;
      return true;
    }

    return false;
  }

  void remove(const std::string& key, const std::string& value) {
    Contends.erase(key);
  }

};

#endif  // MINISQL_TRANSACTION_H
