#include<thread>
#include<mutex>
#include<condition_variable>
#include<future>
#include<vector>
#include<queue>
#include<functional>
#include<stdexcept>
class ThreadPool {
public:
		ThreadPool(size_t max_qu, size_t thread_num) : stop(false), max_que(max_qu) {
		if (max_qu == 0 || thread_num == 0) {
			throw std::invalid_argument("max_que and thread_num must be > 0");
		}
		try {
			works.reserve(thread_num);
			for (size_t i = 0; i < thread_num; ++i) {
				works.emplace_back([this] {
					while (true) {
						std::function<void()> task;
						{
							std::unique_lock<std::mutex> lock(mtx);
							cond_empty.wait(lock, [this] {return stop || !tasks.empty();});
							if (stop && tasks.empty())
								return;
							task = std::move(tasks.front());
							tasks.pop();
							cond_full.notify_one();
						}
						task();
					}
				});
			}
		}
		catch (...) {
			{
				std::lock_guard<std::mutex> lock(mtx);
				stop = true;
			}
			cond_empty.notify_all();
			for (auto& w : works) {
				if (w.joinable())
					w.join();
			}
			throw;
		}
	}
	~ThreadPool() {
		{
			std::unique_lock<std::mutex> lock(mtx);
			stop = true;
			cond_empty.notify_all();
			cond_full.notify_all();
		}

		for (auto& p : works) {
			if (p.joinable())
				p.join();

		}
	}
	template<class F,class ...Args>
	auto addtask(F&& f, Args&&...args) {
		using returntype = decltype(std::forward<F>(f)(std::forward<Args>(args)...));
		auto task =
			std::make_shared<std::packaged_task<returntype()>>
			(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
		std::future<returntype> fut = task->get_future();
		{
			std::unique_lock<std::mutex> lock(mtx);
			if(tasks.size() >= max_que){
				lock.unlock();
				(*task)();
				return fut;
			}
			if (stop) {
				throw std::runtime_error("ThreadPool stopped, cannot add task");
			}
			tasks.emplace([task] {(*task)();});
			cond_empty.notify_one();

		}
		return fut;


	 }


private:
	std::vector<std::thread> works;
	std::queue<std::function<void()>> tasks;
	std::mutex mtx;
	std::condition_variable cond_empty;
	std::condition_variable cond_full;
	bool stop;
	size_t max_que;

};