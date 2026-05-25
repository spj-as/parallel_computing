#include "tasksys.h"
#include <atomic>
#include <vector>
#include <thread>
using namespace std;
IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads),  num_threads_(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    atomic<int> next_task_id(0);
    vector<thread> workers;
    int num_workers = num_threads_;
    for (int i = 0; i < num_workers; i++) {
        workers.emplace_back([&]() {
            while (true) {
                int task_id = next_task_id.fetch_add(1);
                if (task_id >= num_total_tasks) {
                    break;
                }
                runnable->runTask(task_id, num_total_tasks);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads),
        num_threads_(num_threads),
        shutdown_(false),
        has_work_(false),
        next_task_id_(0),
        finished_tasks_(0),
        current_runnable_(nullptr),
        current_num_tasks_(0)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int t = 0; t < num_threads_; t++) {
        workers_.emplace_back([this]() {
            while (!shutdown_) {
                if (!has_work_) {
                    continue;
                }
                int task_id = next_task_id_.fetch_add(1);
                if (task_id < current_num_tasks_) {
                    current_runnable_->runTask(task_id, current_num_tasks_);
                    finished_tasks_.fetch_add(1);
                }
            }
        });
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shutdown_ = true;
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    current_runnable_ = runnable;
    current_num_tasks_ = num_total_tasks;
    next_task_id_ = 0;
    finished_tasks_ = 0;
    has_work_ = true;
    while (finished_tasks_ < num_total_tasks) {

        // spin wait

    }
    has_work_ = false;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
        num_threads_(num_threads),
        shutdown_(false),
        has_work_(false),
        next_task_id_(0),
        finished_tasks_(0),
        current_runnable_(nullptr),
        current_num_tasks_(0)
{
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int t = 0; t < num_threads_; t++) {
        workers_.emplace_back([this]() {
            while (true) {
                int task_id;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    work_cv_.wait(lock, [this]() {
                        return has_work_ || shutdown_;
                    });
                    if (shutdown_) {
                        return;
                    }
                    task_id = next_task_id_++;
                    if (task_id >= current_num_tasks_) {
                        continue;
                    }
                }
                current_runnable_->runTask(task_id, current_num_tasks_);
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    finished_tasks_++;
                    if (finished_tasks_ == current_num_tasks_) {
                        has_work_ = false;
                        done_cv_.notify_one();
                    }
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::unique_lock<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    work_cv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    {
        std::unique_lock<std::mutex> lock(mutex_);
        current_runnable_ = runnable;
        current_num_tasks_ = num_total_tasks;
        next_task_id_ = 0;
        finished_tasks_ = 0;
        has_work_ = true;
    }

    work_cv_.notify_all();
    {
        std::unique_lock<std::mutex> lock(mutex_);
        done_cv_.wait(lock, [this]() {
            return finished_tasks_ == current_num_tasks_;
        });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
