#include <vector>
#include <string>
#include <fstream>
#include <string>
#include <sstream>
#include "Table.hpp"

Table dataloader(std::string path) {
    std::vector<std::string> attr;
    std::vector<std::vector<std::string>> value;
    std::ifstream fp(path); //定义声明一个ifstream对象，指定文件路径
    std::string line;
    int num_item = 0;


    while (getline(fp, line)) { //循环读取每行数据
        std::vector<std::string> data_line;
        std::string data;
        std::istringstream readstr(line); //string数据流化

        //if (num_item > 40000) break;

        // 头文件存入attr
        if (num_item == 0) {
            while (getline(readstr, data, ',')) {
                attr.push_back(data);
            }
            num_item++;
            continue;
        }

        //数据按','分割
        while (getline(readstr, data, ',')) {
            data_line.push_back(data);
        }
        value.push_back(data_line); //插入到vector中
        num_item++;
    }
    return Table(value, attr);
}