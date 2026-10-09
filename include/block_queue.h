#include<mutex>
#include<list>
#include<condition_variable>
template<class T>
class Block_que {
private:
	int que_max_size;
	std::list<T> block_que;
	std::mutex mtx;
	std::condition_variable cond_full;
	std::condition_variable cond_empty;
	bool stop;


	void clean(){
		block_que.clear();

	}

public:
	Block_que() = delete;
	Block_que(const Block_que&) = delete;
	Block_que(int max_q_size):que_max_size(max_q_size),stop(false){}
	Block_que& operator=(const Block_que&) = delete;






	bool push(const T& item) {
		std::unique_lock<std::mutex> lock(mtx);
		cond_full.wait(lock, [this] {return stop || block_que.size() < que_max_size;});
		if (stop){
			this->clean();
			return false;
		}
		block_que.emplace_back(item);
		cond_empty.notify_one();
		return true;
	}
	bool push(T&& item) {
		std::unique_lock<std::mutex> lock(mtx);
		cond_full.wait(lock, [this] {return stop || block_que.size() < que_max_size;});
		if (stop){
			return false;
		}
		block_que.emplace_back(std::forward<T>(item));
		cond_empty.notify_one();
		return true;
	}

	bool pop(T &item) {
		std::unique_lock<std::mutex> lock(mtx);
		cond_empty.wait(lock, [this] {return stop || !block_que.empty();});
		if (stop){
			return false;
		}
		item = block_que.front();
		block_que.pop_front();
		cond_full.notify_one();
		return true;

	}

	void Stop() {
		{
			std::lock_guard<std::mutex> lock(mtx);
			stop = true;
			this->clean();
		}
		cond_empty.notify_all();
		cond_full.notify_all();


	}

	int Getsize() {
		std::lock_guard<std::mutex> lock(mtx);
		return block_que.size();
	}

	

	~Block_que() {
		Stop();
	}

};



