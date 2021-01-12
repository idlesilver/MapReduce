#include <vector>
#include <string>
#include <stack>
#include <algorithm>

class FullReducer
{
public:
	FullReducer(std::vector<std::vector<std::string>> a) : relations(a) {}
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> build()
		// [ ( [A,B,C], [A,C,D] ), ( [A,B,C], [B,C,D] ), ( [A,B,C], [E,C,D] )]
	{
		while (relations.size() != 1)
		{
			int i = 0;
			int consume = 0;
			for (; i < relations.size(); ++i)
			{
				bool is_ear = false;
				std::vector<int> not_unique;
				for (int j = 0; j < relations.size(); ++j)
				{
					// 不找自己
					if (j != i)
						for (int k = 0; k < relations[i].size(); ++k)
						{
							if (std::find(relations[j].begin(), relations[j].end(), relations[i][k]) != relations[j].end())
								// 在别的relations[j]里找到了当前relations[i]的attribute[k]，那么把这个非独有的attr记录下来
								not_unique.push_back(k);
						}
				}
				for (int j = 0; j < relations.size(); ++j)
				{
					if (j != i)
					{
						bool all_contain = true;
						for (int k : not_unique)
						{
							// 对当前relation[i]记录下来的所有非独有元素，在某一个relations[j]中都能找到，记录为all_contain
							if (std::find(relations[j].begin(), relations[j].end(), relations[i][k]) == relations[j].end())
								all_contain = false;
						}
						if (all_contain)
						{
							// 如果某个relations[j]包含所有relations[i]的非独有元素，则[i]记录为ear，[j]记录为其consume
							is_ear = true;
							consume = j;
						}
					}
				}
				if (is_ear)
					// 对每一个relations[i]，被确定为ear并找到consume就跳出循环
					break;
			}
			// 因为只能处理acyclicity的图，所以必须是能找到ear的
			// 所以最终都移除会当前relation（找到consumer，变成ear）
			front.push_back({ relations[consume], relations[i] });	// 正序压入当前relations[i]的(consume,ear)对
			back.push({ relations[i], relations[consume] });		// 逆序压入当前relations[i]的(ear,consume)对
			relations.erase(relations.begin() + i);		// 删除当前relations[i]的所有数据
		}
		std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>
			list(front.begin(), front.end());
		while (!back.empty())
		{
			list.push_back(back.top());
			back.pop();
		}
		return list; 
	}
private:
	std::vector<std::vector<std::string>> relations;
	std::stack<std::pair<std::vector<std::string>, std::vector<std::string>>> back;		// 记录逆序的（ear,consume）对
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> front;	// 记录正序的（consume,ear）对
};