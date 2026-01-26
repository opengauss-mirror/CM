/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
 * ATF is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * ThreadPool.h
 *    Relevant Definitions in the ATF Thread Pool Header File
 *
 * IDENTIFICATION
 *    ATF/ThreadPool.h
 *
 * -------------------------------------------------------------------------
 */
// ThreadPool.h
#ifndef THREADPOOL_H
#define THREADPOOL_H

namespace atf {

#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <thread>
#include <atomic>

class ThreadPool {
public:
    ThreadPool();
    ThreadPool(size_t threadCount);
    ~ThreadPool();
    void Submit(std::function<void()> task);
    void Start(size_t threadCount);
    void Stop();
private:
    void Worker();
    std::vector<std::thread> mThreads;
    std::queue<std::function<void()>> mTasks;
    std::mutex mMutex;
    std::condition_variable mCondition;
    std::atomic<bool> mRunning;
};

}

#endif // THREADPOOL_H