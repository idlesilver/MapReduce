#pragma once
#include <utility>
#include <numeric>
#include <boost/mpi/communicator.hpp>
#include "Context.hpp"

namespace mpi = boost::mpi;

template<typename Task>
class MapReduce
{
public:
	using InputType = typename Task::InputType;
	using SplitType = typename Task::SplitType;
	using MapKeyType = typename Task::MapKeyType;
	using MapValueType = typename Task::MapValueType;
	using ReduceKeyType = typename Task::ReduceKeyType;
	using ReduceValueType = typename Task::ReduceValueType;
	using MapContextType = std::vector<std::pair<MapKeyType, MapValueType>>;
	using CombineType = std::vector<std::pair<MapKeyType, std::vector<MapValueType>>>;

	MapReduce(mpi::communicator w, Task t) : world(&w), task(t) {}

	Table start(const InputType& t)
	{
		auto size = world->size();
		auto rank = world->rank();
		SplitType semi_table;
		// map阶段：把table分为多份，传给从者（master，即rank0处理最后一份数据）
		if (rank == 0)
		{
			for (int i = 1; i < size; ++i)
			{
				world->send(i, 0, task.split(t, size, i));
			}
			semi_table = task.split(t, size, rank);
		}
		else
		{
			world->recv(0, 0, semi_table);
		}
		//Table(semi_table).print();
		// 从者处理各部分数据，并且都写到各线程的实例c中
		task.map(semi_table, c);

		// 从实例c中提取map的总结果，传到master
		auto map_res = c.get_map_context();  //  MapContextType = std::vector<std::pair<MapKeyType, MapValueType>>;
		if (rank == 0)
		{
			MapContextType other;
			for (int i = 1; i < size; ++i)
			{
				world->recv(i, 1, other);
				map_res.insert(map_res.end(), other.begin(), other.end());
			}
		}
		else
		{
			world->send(0, 1, map_res);
		}

		// 把map的总结果合成一个，并重新分配（shuffle）准备做reduce
		CombineType combine_res;
		if (rank == 0)
		{
			combine_res = combine(map_res);
			//print_combine(combine_res);
			// NOTE: 这里的shuffle不是均分的，不同key下内容量不同
			for(int i = 1; i < size; ++i)
			{
				auto begin = combine_res.begin() + combine_res.size() / size * i;
				auto end = combine_res.begin() + combine_res.size() / size * (i + 1);
				world->send(i, 2, CombineType(begin, size - 1 == i ? combine_res.end() : end));
			}
			combine_res = CombineType(combine_res.begin(), combine_res.begin() + combine_res.size() / size); // 最后一份留给自己做
		}
		else
		{
			world->recv(0, 2, combine_res);
		}

		// 根据分配到的 （combine的结果）做reduce
		for (auto& p : combine_res)
		{
			task.reduce(p.first, p.second, c);
		}
		auto reduce_res = c.get_reduce_context();

		// 合并所有的reduce结果
		if (rank == 0)
		{
			std::vector<std::pair<ReduceKeyType, ReduceValueType>> other;
			for (int i = 1; i < size; ++i)
			{
				world->recv(i, 3, other);
				reduce_res.insert(reduce_res.end(), other.begin(), other.end());
			}
			//Table res = reduce_res.front.second;
			ReduceValueType res = std::accumulate(
				reduce_res.begin(), reduce_res.end(), 
				ReduceValueType(), 
				[](auto a, auto b) {return a + b.second; }); // 表不断变大，不停插入原始tuple（所有的value），加号重新定义，是v（value即tuple）的扩展
			//res.print();
			return res;
		}
		else
		{
			world->send(0, 3, reduce_res);
			return Table();
		}
	}

	CombineType combine(MapContextType& map_context)
	{
		//  MapContextType = std::vector<std::pair<MapKeyType, MapValueType>>;
		std::sort(map_context.begin(), map_context.end(), [](auto a, auto b) {return a.first > b.first; }); // 按照key来排序，keytype是vector<string>
		MapKeyType curr = map_context.front().first; // 当前的key。TODO: 如果某个key只存在于table1中，对semijoin而言可以直接省略
		CombineType res;
		std::vector<MapValueType> same_key;	// 有相同key的values（这里的value是：(0, ("a1", "b1", "c1") ) / (1, 空vector)  ）
		for (auto i = map_context.begin(); i != map_context.end(); ++i)
		{
			if (i->first == curr)
			{
				same_key.push_back(i->second);
			}
			else
			{
				res.emplace_back(curr, same_key); // 当前的key和其所有values
				same_key.clear();
				same_key.push_back(i->second);
				curr = i->first;
			}
		}
		if (!same_key.empty())
		{
			res.emplace_back(curr, same_key);
		}
		return res;
	}

	void print_combine(const CombineType& c)
	{
		for (auto& p : c)
		{
			std::cout << "key: ";
			for (auto& k : p.first)
			{
				std::cout << k;
			}
			std::cout << "value: ";
			for (auto& v : p.second)
			{
				std::cout << "Table: " << v.first << ' ';
				for (auto& i : v.second)
				{
					std::cout << i <<' ';
				}
			}
			std::cout << std::endl;
		}
	}

private:
	mpi::communicator* world;
	//Table table;
	Task task;
	Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType> c; // c的作用是记录map和reduce
};
