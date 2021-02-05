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

	// acyclicity的reducer，优化属性的个数
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> list;
	if (world->rank() == 0) // 只有master需要做acyclicity的reduce，并储存到list中
	{
		for (const auto& t : input)
		{
			attrs.push_back(t.attribute());
		}
		FullReducer reducer(attrs);
		list = reducer.build(); // 返回的list是做semijoin的顺序
		for (int i = 1; i < world->size(); ++i)	//把list的结果传递到所有机器上
		{
			world->send(i, 0, list);
		}


		//// 显示做semijoin的表的属性值 =================
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
		//// 显示semijoin优化前的原表
		//std::cout << "original tables before semijoin" << std::endl;
		//for (auto t : input)
		//	t.print();

	}
	else
	{
		world->recv(0, 0, list);
	}



	// 根据reducer给的顺序做semijoin，优化dangling items
	// 每次对full reducer list中的一对(consumer,ear)做semijoin
	for (const auto& p : list)
	{
		// 找到两个relation中共有的属性
		std::vector<int> param1;
		std::vector<int> param2;
		for (int i = 0; i < p.first.size(); ++i)
		{
			for (int j = 0; j < p.second.size(); ++j)
			{
				if (p.first[i] == p.second[j])
				{
					// 只找相同的attr做semijoin
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

		// 从table中找到(consumer,ear)对应的原relation (attr & tuples)
		// 这里的consumer,ear只有attr的记录
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

		// 执行优化的semijoin（attr数量已经减少了，即param1和param2）
		MapReduce<SemiJoiner> sj(*world, SemiJoiner(param1, param2));
		Table res;
		if (world->rank() == 0)
		{
			res = sj.start({ input[t1], input[t2] });

			//// 显示semijoin更新后的左表=================
			//std::cout << "semi-join " << t1 << " <-- " << t2 << std::endl;
			//res.print();

			// 参与semijoin的左表更新（删掉了dangling items
			input[t1].update(std::move(res));

		}
		else
		{
			sj.start({});
		}
	}

	//if (world->rank() == 0) {
	//	// 显示semijoin优化后的原表
	//	std::cout << "semi-join over" << std::endl;
	//	std::cout << "optimized tables after semijoin" << std::endl;
	//	for (auto t : input)
	//		t.print();
	//}

	// 通过semijoin后，dangling items都没了，开始做join（此处是natural join）
	std::vector<std::string> attr;
	size_t size;
	Table table;
	if (world->rank() == 0)
	{
		// 指定join的对象：表0
		attr = input[0].attribute();
		size = input.size();
		for (int i = 1; i < world->size(); ++i)
		{
			world->send(i, 0, attr);
		}
		table = input[0];
		// 显示目标表（表1）
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

	// 其余每个表都和目标表（表1）分别做join
	for (int i = 1; i < size; ++i)
	{
		std::vector<int> param1;
		std::vector<int> param2;
		std::vector<std::string> attr2;
		if (world->rank() == 0)
		{
			attr2 = input[i].attribute(); // 把当前表2的属性传到所有slave上
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
					// 只找相同的attr做semijoin
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
			table.update(res);// 只有master处的表1要更新（每次做join，目标表属性只可能变大
		}
		else
		{
			sj.start({});
		}
		//res.print();
		//std::cout << "join " << world->rank() << std::endl;
		// 更新表，扩展属性之后的表头
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
	// semijoin优化
	start = clock();		//程序开始计时
	for (int i = 0; i < num_loop; i++) {
		optimize_join(tables,&world);
	}
	end = clock();		//程序结束用时
	endtime = ((double)end - (double)start) / CLOCKS_PER_SEC;
	if (world.rank() == 0) {
		//std::cout << "semijoin:" << endtime << std::endl;		//s为单位
		std::cout << "semijoin optimization:" << endtime * 1000 << "ms" << std::endl;	//ms为单位
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

	// 无优化
	start = clock();		//程序开始计时
	for (int i = 0; i < num_loop; i++) {
		join(tables, &world);
	}
	end = clock();		//程序结束用时
	endtime = ((double)end - (double)start) / CLOCKS_PER_SEC;
	if (world.rank() == 0) {
		//std::cout << "Total time:" << endtime << std::endl;		//s为单位
		std::cout << "no optimization:" << endtime * 1000 << "ms" << std::endl;	//ms为单位
		std::cout << std::endl;
	}


	return 0;
}



