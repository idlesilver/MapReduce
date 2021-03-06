#pragma once
#include "Context.hpp"

class SemiJoiner
{
public:
	SemiJoiner(std::vector<int> l1, std::vector<int> l2): first(l1), second(l2) 
	{}
	SemiJoiner(const SemiJoiner&) = default;

	using InputType = std::pair<Table, Table>;
	using SplitType = std::vector<std::pair<int, std::vector<std::string>>>;
	using MapKeyType = std::vector<std::string>;
	using MapValueType = std::pair<int, std::vector<std::string>>;
	using ReduceKeyType = MapKeyType;
	using ReduceValueType = Table;

	SplitType split(const InputType& input, int size, int rank)
	{
		SplitType res;
		auto first_split = input.first.split(size, rank);
		auto second_split = input.second.split(size, rank);
		// 把要做semijoin的两张表的tuple合并成一个vector，并用int0/1来标记表来源
		for (auto& i : first_split)
		{
			res.emplace_back(0, i);
		}
		for (auto& i : second_split)
		{
			res.emplace_back(1, i);
		}
		return res;
	}

	void map(SplitType value, Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		for (auto& row : value)
		{
			if (row.first == 0) // 来自第一张表
			//  (0, ("a1", "b1", "c1") )
			{
				MapKeyType new_key;
				std::vector<std::string> new_value;
				for (int i = 0; i < row.second.size(); ++i) // 遍历第一张表的tuple（i是attr的index
				{
					if (std::find(first.begin(), first.end(), i) != first.end()) // 如果attr在化简后的attr_list中
					{
						new_key.push_back(row.second[i]); // key记录的是化简后的attr对应的value
					}
					new_value.push_back(row.second[i]); // value记录的是所有的value
				}
				c.write(new_key, std::pair<int, std::vector<std::string>>(0, new_value));
				//    ("a1", "c1")									(0, ("a1", "b1", "c1") )
			}
			if (row.first == 1) // 来自第二张表
			//  (1, ("a1", "d1") )
			{
				MapKeyType new_key;
				for (int i = 0; i < row.second.size(); ++i)
				{
					if (std::find(second.begin(), second.end(), i) != second.end())
					{
						new_key.push_back(row.second[i]); // 只记录化简后的attr对应的value
					}
				}
				c.write(new_key, std::pair<int, std::vector<std::string>>(1, std::vector<std::string>{}));
				//    ("a1")											(1, 空vector)
			}
		}
	}

	void reduce(MapKeyType key, std::vector<MapValueType> value,
		Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		std::vector<MapKeyType> first_table;
		std::vector<MapKeyType> second_table;
		for (auto& val : value)
		{
			if (val.first == 0)
				first_table.push_back(val.second);
			else
				second_table.push_back(val.second); // 这里放入的都是  空vector
		}
		if (first_table.empty() || second_table.empty()) // 如果对于同一个key，两表中有一表无数据，则全部内容被reduce掉（reduce dangling items）
			return;
		ReduceValueType table;
		for (auto& i : first_table)
		{
			table.push_back(std::move(i));
		}
		c.write(key, table);
	}
private:
	std::vector<int> first;	// 第一张表的化简后attr的index
	std::vector<int> second;// 第一张表的化简后attr的index
};