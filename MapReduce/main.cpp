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

void optimize_join(std::vector<Table>& input, mpi::communicator* world)
{
	//mpi::communicator world;
	std::vector<std::vector<std::string>> attrs;

	// acyclicity��reducer���Ż����Եĸ���
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> list;
	if (world->rank() == 0) // ֻ��master��Ҫ��acyclicity��reduce�������浽list��
	{
		for (const auto& t : input)
		{
			attrs.push_back(t.attribute());
		}
		FullReducer reducer(attrs);
		list = reducer.build(); // ���ص�list����semijoin��˳��
		for (int i = 1; i < world->size(); ++i)	//��list�Ľ�����ݵ����л�����
		{
			world->send(i, 0, list);
		}


		//// ��ʾ��semijoin�ı������ֵ =================
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
		//// ��ʾsemijoin�Ż�ǰ��ԭ��
		//std::cout << "original tables before semijoin" << std::endl;
		//for (auto t : input)
		//	t.print();

	}
	else
	{
		world->recv(0, 0, list);
	}



	// ����reducer����˳����semijoin���Ż�dangling items
	// ÿ�ζ�full reducer list�е�һ��(consumer,ear)��semijoin
	for (const auto& p : list)
	{
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
		//for (int i : param1)
		//	std::cout << i << ' ';
		//std::cout << std::endl;
		//for (int i : param2)
		//	std::cout << i << ' ';
		//std::cout << std::endl;

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
		MapReduce<SemiJoiner> sj(*world, SemiJoiner(param1, param2));
		Table res;
		if (world->rank() == 0)
		{
			res = sj.start({ input[t1], input[t2] });

			//// ��ʾsemijoin���º�����=================
			//std::cout << "semi-join " << t1 << " <-- " << t2 << std::endl;
			//res.print();

			// ����semijoin�������£�ɾ����dangling items
			input[t1].update(std::move(res));

		}
		else
		{
			sj.start({});
		}
	}

	//if (world->rank() == 0) {
	//	// ��ʾsemijoin�Ż����ԭ��
	//	std::cout << "semi-join over" << std::endl;
	//	std::cout << "optimized tables after semijoin" << std::endl;
	//	for (auto t : input)
	//		t.print();
	//}

	// ͨ��semijoin��dangling items��û�ˣ���ʼ��join���˴���natural join��
	std::vector<std::string> attr;
	size_t size;
	Table table;
	if (world->rank() == 0)
	{
		// ָ��join�Ķ��󣺱�0
		attr = input[0].attribute();
		size = input.size();
		for (int i = 1; i < world->size(); ++i)
		{
			world->send(i, 0, attr);
		}
		table = input[0];
		// ��ʾĿ�����1��
		//table.print();

		for (int i = 1; i < world->size(); ++i)
		{
			world->send(i, 1, size);
		}
	}
	else
	{
		world->recv(0, 0, attr);
		world->recv(0, 1, size);
	}

	// ����ÿ������Ŀ�����1���ֱ���join
	for (int i = 1; i < size; ++i)
	{
		std::vector<int> param1;
		std::vector<int> param2;
		std::vector<std::string> attr2;
		if (world->rank() == 0)
		{
			attr2 = input[i].attribute(); // �ѵ�ǰ��2�����Դ�������slave��
			for (int j = 1; j < world->size(); ++j)
			{
				world->send(j, 0, input[i].attribute());
			}
		}
		else
		{
			world->recv(0, 0, attr2);
		}
		//auto attr2 = input[i].attribute();
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
		MapReduce<Joiner> sj(*world, Joiner(param1, param2));
		Table res;
		if (world->rank() == 0)
		{
			res = sj.start({ table, input[i] });
			table.update(res);// ֻ��master���ı�1Ҫ���£�ÿ����join��Ŀ�������ֻ���ܱ��
		}
		else
		{
			sj.start({});
		}
		//res.print();
		//std::cout << "join " << world->rank() << std::endl;
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
		/*for (auto a : new_attr)
		{
			std::cout << a << ' ';
		}
		std::cout << std::endl;*/
		attr = new_attr;
	}
	//if (world->rank() == 0)
		//table.print();
}

void join(std::vector<Table>& input, mpi::communicator* world)
{
	//mpi::communicator world;
	std::vector<std::vector<std::string>> attrs;
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> list;

	//std::cout << "semi-join over"<<std::endl;
	std::vector<std::string> attr;
	size_t size;
	Table table;
	if (world->rank() == 0)
	{
		attr = input[0].attribute();
		size = input.size();
		for (int i = 1; i < world->size(); ++i)
		{
			world->send(i, 0, attr);
		}
		table = input[0];
		//table.print();
		for (int i = 1; i < world->size(); ++i)
		{
			world->send(i, 1, size);
		}
	}
	else
	{
		world->recv(0, 0, attr);
		world->recv(0, 1, size);
	}
	for (int i = 1; i < size; ++i)
	{
		std::vector<int> param1;
		std::vector<int> param2;
		std::vector<std::string> attr2;
		if (world->rank() == 0)
		{
			attr2 = input[i].attribute();
			for (int j = 1; j < world->size(); ++j)
			{
				world->send(j, 0, input[i].attribute());
			}
		}
		else
		{
			world->recv(0, 0, attr2);
		}
		//auto attr2 = input[i].attribute();
		for (int j = 0; j < attr.size(); ++j)
		{
			for (int k = 0; k < attr2.size(); ++k)
			{
				if (attr[j] == attr2[k])
				{
					param1.push_back(j);
					param2.push_back(k);
				}
			}
		}
		MapReduce<Joiner> sj(*world, Joiner(param1, param2));
		Table res;
		if (world->rank() == 0)
		{
			res = sj.start({ table, input[i] });
			table.update(res);
		}
		else
		{
			sj.start({});
		}
		//res.print();
		//std::cout << "join " << world->rank() << std::endl;
		std::vector<std::string> new_attr;
		for (int j = 0; j < param1.size(); ++j)
			new_attr.push_back(attr[param1[j]]);
		for (int j = 0; j < attr.size(); ++j)
			if (std::find(param1.begin(), param1.end(), j) == param1.end())
				new_attr.push_back(attr[j]);
		for (int j = 0; j < attr2.size(); ++j)
			if (std::find(param2.begin(), param2.end(), j) == param2.end())
				new_attr.push_back(attr2[j]);
		/*for (auto a : new_attr)
		{
			std::cout << a << ' ';
		}
		std::cout << std::endl;*/
		attr = new_attr;
	}
	//if (world->rank() == 0)
		//table.print();
}

int main()
{
	mpi::environment env;
	mpi::communicator world;
	
	std::cout << world.rank() << " in " << world.size() << std::endl;


	Table base;
	Table trans;
	std::vector<Table> tables;
	if (world.rank() == 0) {
		//Table base = dataloader("C:\\Users\\51284\\Desktop\\train_base.csv");
		//Table ops = dataloader("C:\\Users\\51284\\Desktop\\train_op.csv");
		//Table trans = dataloader("C:\\Users\\51284\\Desktop\\train_trans.csv");
		//Table lable = dataloader("C:\\Users\\51284\\Desktop\\train_lable.csv");
		base = dataloader("C:\\Users\\51284\\Desktop\\base.csv");
		trans = dataloader("C:\\Users\\51284\\Desktop\\trans.csv");
		tables = std::vector<Table>{ base, trans };
	}
	else {
		auto test1 = Table(std::vector<std::vector<std::string>>
		{
				std::vector<std::string>{"6800", "24877"},
				std::vector<std::string>{"23487", "24895"},
		}, { "user", "age" });
		tables = std::vector<Table>{ Table(),Table() };
	}


	int num_loop = 1;
	clock_t start, end;
	double endtime;
	// semijoin�Ż�
	start = clock();		//����ʼ��ʱ
	for (int i = 0; i < num_loop; i++) {
		optimize_join(tables,&world);
	}
	end = clock();		//���������ʱ
	endtime = ((double)end - (double)start) / CLOCKS_PER_SEC;
	if (world.rank() == 0) {
		//std::cout << "semijoin:" << endtime << std::endl;		//sΪ��λ
		std::cout << "semijoin optimization:" << endtime * 1000 << "ms" << std::endl;	//msΪ��λ
	}


	if (world.rank() == 0) {
		//Table base = dataloader("C:\\Users\\51284\\Desktop\\train_base.csv");
		//Table ops = dataloader("C:\\Users\\51284\\Desktop\\train_op.csv");
		//Table trans = dataloader("C:\\Users\\51284\\Desktop\\train_trans.csv");
		//Table lable = dataloader("C:\\Users\\51284\\Desktop\\train_lable.csv");
		base = dataloader("C:\\Users\\51284\\Desktop\\base.csv");
		trans = dataloader("C:\\Users\\51284\\Desktop\\trans.csv");
		tables = std::vector<Table>{ base, trans };
	}
	else {
		auto test1 = Table(std::vector<std::vector<std::string>>
		{
			std::vector<std::string>{"6800", "24877"},
				std::vector<std::string>{"23487", "24895"},
		}, { "user", "age" });
		tables = std::vector<Table>{ Table(),Table() };
	}

	// ���Ż�
	start = clock();		//����ʼ��ʱ
	for (int i = 0; i < num_loop; i++) {
		join(tables, &world);
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



