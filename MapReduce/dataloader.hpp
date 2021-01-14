#include <vector>
#include <string>
#include <fstream>
#include <string>
#include <sstream>
#include "Table.hpp"

Table dataloader(std::string path) {
    std::vector<std::string> attr;
    std::vector<std::vector<std::string>> value;
    std::ifstream fp(path); //��������һ��ifstream����ָ���ļ�·��
    std::string line;
    int num_item = 0;


    while (getline(fp, line)) { //ѭ����ȡÿ������
        std::vector<std::string> data_line;
        std::string data;
        std::istringstream readstr(line); //string��������

        //if (num_item > 40000) break;

        // ͷ�ļ�����attr
        if (num_item == 0) {
            while (getline(readstr, data, ',')) {
                attr.push_back(data);
            }
            num_item++;
            continue;
        }

        //���ݰ�','�ָ�
        while (getline(readstr, data, ',')) {
            data_line.push_back(data);
        }
        value.push_back(data_line); //���뵽vector��
        num_item++;
    }
    return Table(value, attr);
}