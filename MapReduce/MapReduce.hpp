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
		// map�׶Σ���table��Ϊ��ݣ��������ߣ�master����rank0�������һ�����ݣ�
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
		// ���ߴ�����������ݣ����Ҷ�д�����̵߳�ʵ��c��
		task.map(semi_table, c);

		// ��ʵ��c����ȡmap���ܽ��������master
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

		// ��map���ܽ���ϳ�һ���������·��䣨shuffle��׼����reduce
		CombineType combine_res;
		if (rank == 0)
		{
			combine_res = combine(map_res);
			//print_combine(combine_res);
			// NOTE: �����shuffle���Ǿ��ֵģ���ͬkey����������ͬ
			for(int i = 1; i < size; ++i)
			{
				auto begin = combine_res.begin() + combine_res.size() / size * i;
				auto end = combine_res.begin() + combine_res.size() / size * (i + 1);
				world->send(i, 2, CombineType(begin, size - 1 == i ? combine_res.end() : end));
			}
			combine_res = CombineType(combine_res.begin(), combine_res.begin() + combine_res.size() / size); // ���һ�������Լ���
		}
		else
		{
			world->recv(0, 2, combine_res);
		}

		// ���ݷ��䵽�� ��combine�Ľ������reduce
		for (auto& p : combine_res)
		{
			task.reduce(p.first, p.second, c);
		}
		auto reduce_res = c.get_reduce_context();

		// �ϲ����е�reduce���
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
				[](auto a, auto b) {return a + b.second; }); // ���ϱ�󣬲�ͣ����ԭʼtuple�����е�value�����Ӻ����¶��壬��v��value��tuple������չ
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
		std::sort(map_context.begin(), map_context.end(), [](auto a, auto b) {return a.first > b.first; }); // ����key������keytype��vector<string>
		MapKeyType curr = map_context.front().first; // ��ǰ��key��TODO: ���ĳ��keyֻ������table1�У���semijoin���Կ���ֱ��ʡ��
		CombineType res;
		std::vector<MapValueType> same_key;	// ����ͬkey��values�������value�ǣ�(0, ("a1", "b1", "c1") ) / (1, ��vector)  ��
		for (auto i = map_context.begin(); i != map_context.end(); ++i)
		{
			if (i->first == curr)
			{
				same_key.push_back(i->second);
			}
			else
			{
				res.emplace_back(curr, same_key); // ��ǰ��key��������values
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
	Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType> c; // c�������Ǽ�¼map��reduce
};
