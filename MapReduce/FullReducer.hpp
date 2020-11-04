#include <vector>
#include <string>
#include <stack>
#include <algorithm>

class FullReducer
{
public:
	FullReducer(std::vector<std::vector<std::string>> a) : relations(a) {}
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> build()
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
								// 在别的relations里找到了当前relations[i]的attribute[k]，那么把这个非独有的attr记录下来
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
							// 对当前relation[i]记录下来的所有非独有元素，检查是不是在relations[j]中都有
							if (std::find(relations[j].begin(), relations[j].end(), relations[i][k]) == relations[j].end())
								all_contain = false;
						}
						if (all_contain)
						{
							// 如果relations[j]包含所有relations[i]的非独有元素，则[i]记录为ear，[j]记录为其consume
							is_ear = true;
							consume = j;
						}
					}
				}
				if (is_ear)
					// 确定为ear并找到consume就跳出循环
					break;
			}
			// ？？？不管怎样都移除当前relation？？？
			// 应该是只移除ear？
			front.push_back({ relations[consume], relations[i] });
			back.push({ relations[i], relations[consume] });
			relations.erase(relations.begin() + i);
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
	// ？？？ back有什么作用吗？
	std::stack<std::pair<std::vector<std::string>, std::vector<std::string>>> back;
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> front;
};