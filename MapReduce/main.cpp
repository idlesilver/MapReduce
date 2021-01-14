#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <iostream>
//#include<cstdlib>
#include<ctime>

#include "Table.hpp"
#include "MapReduce.hpp"
#include "Selector.hpp"
#include "Projector.hpp"
#include "Joiner.hpp"
#include "SemiJoin.hpp"
#include "FullReducer.hpp"
#include "dataloader.hpp"

namespace mpi = boost::mpi;

void optimize_join(std::vector<Table>& input)
{
	mpi::communicator world;
	std::vector<std::vector<std::string>> attrs;

	// acyclicity��reducer���Ż����Եĸ���
	for (const auto& t : input)
	{
		attrs.push_back(t.attribute());
	}
	FullReducer reducer(attrs);
	auto list = reducer.build(); // ���ص�list����semijoin��˳��

	//// ��ʾ��semijoin�ı������ֵ��
	//std::cout << "attributes of tables, in the order of semijoin sequence" << std::endl;
	//for (auto i : list)
	//{
	//	for (auto j : i.first)
	//		std::cout << j <<' ';
	//	std::cout << '|';
	//	for (auto j : i.second)
	//		std::cout << j << ' ';
	//	std::cout << std::endl;
	//}

	// ����reducer����˳����semijoin���Ż�dangling items
	// ÿ�ζ�full reducer list�е�һ��(consumer,ear)��semijoin
	for (const auto& p : list)
	{
		//// ��ʾsemijoin�Ż�ǰ��ԭ��
		//std::cout << "original tables before semijoin" << std::endl;
		//for (auto t : input)
		//	t.print();

		// �ҵ�����relation�й��е�����
		std::vector<int> param1;
		std::vector<int> param2;
		for (int i = 0; i < p.first.size(); ++i)
		{
			for (int j = 0; j < p.second.size(); ++j)
			{
				if (p.first[i] == p.second[j])
				{
					// ֻ����ͬ��attr��semijoin
					param1.push_back(i);
					param2.push_back(j);
				}
			}
		}
		/*for (int i : param1)
			std::cout << i << ' ';
		std::cout << std::endl;
		for (int i : param2)
			std::cout << i << ' ';
		std::cout << std::endl;*/
		
		// ��table���ҵ�(consumer,ear)��Ӧ��ԭrelation (attr & tuples)
		// �����consumer,earֻ��attr�ļ�¼
		int t1 = 0;
		int t2 = 0;
		for (int i = 0; i < input.size(); ++i)
		{
			if (p.first == input[i].attribute())
				t1 = i;
			if (p.second == input[i].attribute())
				t2 = i;
		}
		//std::cout << t1 << ' ' << t2;

		// ִ���Ż���semijoin��attr�����Ѿ������ˣ���param1��param2��
		MapReduce<SemiJoiner> sj(world, SemiJoiner(param1, param2));
		auto&& res = sj.start({ input[t1], input[t2] });
		//// ��ʾsemijoin���º�����
		//std::cout << "semi-join " << t1 << " <-- " << t2 << std::endl;
		//res.print();

		// ����semijoin�������£�ɾ����dangling  itemsitems
		input[t1].update(std::move(res));
	}

	//// ��ʾsemijoin�Ż����ԭ
	//std::cout << "optimized tables after semijoin" << std::endl;
	//for (auto t : input)
	//	t.print();


	// ͨ��semijoin��dangling items��û�ˣ���ʼ��join���˴���natural join��
	// ָ��join�Ķ��󣺱�0
	int target_table_idx = 0;
	auto attr = input[target_table_idx].attribute();
	auto table = input[target_table_idx];
	// ��ʾĿ���
	//table.print();

	// ����ÿ������Ŀ���ֱ���join
	for (int i = 1; i < input.size(); ++i)
	{
		// �ҵ�����relation�й��е�����
		std::vector<int> param1;
		std::vector<int> param2;
		auto attr2 = input[i].attribute();
		for (int j = 0; j < attr.size(); ++j)
		{
			for (int k = 0; k < attr2.size(); ++k)
			{
				if (attr[j] == attr2[k])
				{
					// ֻ����ͬ��attr��semijoin
					param1.push_back(j);
					param2.push_back(k);
				}
			}
		}
		MapReduce<Joiner> sj(world, Joiner(param1, param2));
		auto&& res = sj.start({ table, input[i] });
		// ���±���չ����֮��ı�ͷ
		std::vector<std::string> new_attr;
		for (int j = 0; j < param1.size(); ++j)
			new_attr.push_back(attr[param1[j]]);
		for (int j = 0; j < attr.size(); ++j)
			if (std::find(param1.begin(), param1.end(), j) == param1.end())
				new_attr.push_back(attr[j]);
		for (int j = 0; j < attr2.size(); ++j)
			if (std::find(param2.begin(), param2.end(), j) == param2.end())
				new_attr.push_back(attr2[j]);

		//// ��ʾjoin��ı�
		//std::cout << "join bewteen table " << target_table_idx << " and " << i << " processed in " << world.rank() << std::endl;
		//for (auto a : new_attr)
		//{
		//	std::cout << a << ' ';
		//}
		//std::cout << std::endl;
		//attr = new_attr;
		//res.print();

		// ���±�ÿ����join��Ŀ�������ֻ���ܱ��
		table.update(res);
	}
	//table.print();
}

void join(std::vector<Table>& input){
	mpi::communicator world;
	// ָ��join�Ķ��󣺱�0
	int target_table_idx = 0;
	auto attr = input[target_table_idx].attribute();
	auto table = input[target_table_idx];
	// ��ʾĿ���
	//table.print();

	// ����ÿ������Ŀ���ֱ���join
	for (int i = 1; i < input.size(); ++i)
	{
		// �ҵ�����relation�й��е�����
		std::vector<int> param1;
		std::vector<int> param2;
		auto attr2 = input[i].attribute();
		for (int j = 0; j < attr.size(); ++j)
		{
			for (int k = 0; k < attr2.size(); ++k)
			{
				if (attr[j] == attr2[k])
				{
					// ֻ����ͬ��attr��semijoin
					param1.push_back(j);
					param2.push_back(k);
				}
			}
		}
		MapReduce<Joiner> sj(world, Joiner(param1, param2));
		auto&& res = sj.start({ table, input[i] });
		// ���±���չ����֮��ı�ͷ
		std::vector<std::string> new_attr;
		for (int j = 0; j < param1.size(); ++j)
			new_attr.push_back(attr[param1[j]]);
		for (int j = 0; j < attr.size(); ++j)
			if (std::find(param1.begin(), param1.end(), j) == param1.end())
				new_attr.push_back(attr[j]);
		for (int j = 0; j < attr2.size(); ++j)
			if (std::find(param2.begin(), param2.end(), j) == param2.end())
				new_attr.push_back(attr2[j]);

		//// ��ʾjoin��ı�
		//std::cout << "join bewteen table " << target_table_idx << " and " << i << " processed in " << world.rank() << std::endl;
		//for (auto a : new_attr)
		//{
		//	std::cout << a << ' ';
		//}
		//std::cout << std::endl;
		//attr = new_attr;
		//res.print();

		// ���±�ÿ����join��Ŀ�������ֻ���ܱ��
		table.update(res);
	}
	//table.print();
}

int main()
{
	mpi::environment env;
	mpi::communicator world;
	
	//auto t = Table(std::vector<std::vector<std::string>>
	//{
	//		std::vector<std::string>{"a", "1", "1"},
	//		std::vector<std::string>{"a", "2", "1"},
	//		std::vector<std::string>{"b", "3", "2"},
	//		std::vector<std::string>{"b", "4", "2"},
	//		std::vector<std::string>{"c", "5", "3"},
	//		std::vector<std::string>{"c", "6", "3"},
	//}, { "A", "B", "C" });
	//auto t2 = Table(std::vector<std::vector<std::string>>
	//{
	//		std::vector<std::string>{"1", "!", "1"},
	//		std::vector<std::string>{"2", "@", "1"},
	//		std::vector<std::string>{"3", "#", "3"},
	//		std::vector<std::string>{"3", "$", "5"},
	//		std::vector<std::string>{"4", "%", "3"},
	//		std::vector<std::string>{"5", "^", "3"},
	//}, { "C", "D","E" });
	//auto t3 = Table(std::vector<std::vector<std::string>>
	//{
	//		std::vector<std::string>{"a", "1", "u"},
	//		std::vector<std::string>{"a", "1", "i"},
	//		std::vector<std::string>{"b", "1", "o"},
	//		std::vector<std::string>{"c", "5", "j"},
	//		std::vector<std::string>{"c", "3", "k"},
	//		std::vector<std::string>{"d", "2", "l"},
	//}, {"A", "E", "F" });
	//auto t4 = Table(std::vector<std::vector<std::string>>
	//{
	//		std::vector<std::string>{"a", "1", "1"},
	//		std::vector<std::string>{"b", "2", "1"},
	//		std::vector<std::string>{"b", "1", "2"},
	//		std::vector<std::string>{"c", "3", "3"},
	//		std::vector<std::string>{"c", "3", "5"},
	//		std::vector<std::string>{"d", "2", "3"},
	//}, { "A", "C", "E" });
	//std::vector<Table> tables = { t, t2, t3, t4 };

	//auto test = Table(std::vector<std::vector<std::string>>
	//{
	//		std::vector<std::string>{"6800", "1.0"},
	//		std::vector<std::string>{"23487", "1.0"},
	//}, { "user", "sex"});

	//auto test1 = Table(std::vector<std::vector<std::string>>
	//{
	//		std::vector<std::string>{"6800", "24877"},
	//		std::vector<std::string>{"23487", "24895"},
	//}, { "user", "age" });

	if (world.rank() == 0) {
		Table base = dataloader("C:\\Users\\51284\\Desktop\\train_base.csv");
		Table ops = dataloader("C:\\Users\\51284\\Desktop\\train_op.csv");
		Table trans = dataloader("C:\\Users\\51284\\Desktop\\train_trans.csv");
		std::vector<Table> tables = { base, ops, trans };
	}


	std::cout << world.rank() << " in " << world.size() << std::endl;


	int num_loop = 1;
	clock_t start, end;
	double endtime;
	// semijoin�Ż�
	start = clock();		//����ʼ��ʱ
	for (int i = 0; i < num_loop; i++) {
		optimize_join(tables);
	}
	end = clock();		//���������ʱ
	endtime = ((double)end - (double)start) / CLOCKS_PER_SEC;
	if (world.rank() == 0) {
		//std::cout << "semijoin:" << endtime << std::endl;		//sΪ��λ
		std::cout << "semijoin optimization:" << endtime * 1000 << "ms" << std::endl;	//msΪ��λ
	}

	// ���Ż�
	start = clock();		//����ʼ��ʱ
	for (int i = 0; i < num_loop; i++) {
		join(tables);
	}
	end = clock();		//���������ʱ
	endtime = ((double)end - (double)start) / CLOCKS_PER_SEC;
	if (world.rank() == 0) {
		//std::cout << "Total time:" << endtime << std::endl;		//sΪ��λ
		std::cout << "no optimization:" << endtime * 1000 << "ms" << std::endl;	//msΪ��λ
		std::cout << std::endl;
	}


	return 0;
}



