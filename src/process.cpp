#include "process.h"
#include <algorithm>

// Process class methods
Process::Process(ProcessDetails details, uint64_t current_time)
{
    int i;
    pid = details.pid;
    start_time = details.start_time;
    num_bursts = details.num_bursts;
    current_burst = 0;
    burst_times = new uint32_t[num_bursts];
    for (i = 0; i < num_bursts; i++)
    {
        burst_times[i] = details.burst_times[i];
    }
    priority = details.priority;
    state = (start_time == 0) ? State::Ready : State::NotStarted;
    if (state == State::Ready)
    {
        launch_time = current_time;
    }
    is_interrupted = false;
    core = -1;
    turn_time = 0;
    wait_time = 0;
    cpu_time = 0;
    burst_start_time = 0;
    completion_time = 0;
    total_time = 0;
    for (i = 0; i < num_bursts; i+=2)
    {
        total_time += burst_times[i];
    }
    remain_time = total_time;
    in_ready_queue = false;
    last_update_time = (state == State::Ready) ? current_time : 0;
}

Process::~Process()
{
    delete[] burst_times;
}

uint16_t Process::getPid() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return pid;
}

uint32_t Process::getStartTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return start_time;
}

uint8_t Process::getPriority() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return priority;
}

uint64_t Process::getBurstStartTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return burst_start_time;
}

uint64_t Process::getCompletionTimeMs() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return completion_time;
}

uint64_t Process::getLastUpdateTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return last_update_time;
}

Process::State Process::getState() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return state;
}

bool Process::isInterrupted() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return is_interrupted;
}

int8_t Process::getCpuCore() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return core;
}

uint32_t Process::getCurrentBurstRemainingMs() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    if (current_burst >= num_bursts)
    {
        return 0;
    }
    return burst_times[current_burst];
}

double Process::getTurnaroundTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    if (completion_time >= launch_time)
    {
        return (double)(completion_time - launch_time) / 1000.0;
    }
    return (double)turn_time / 1000.0;
}

double Process::getWaitTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return (double)wait_time / 1000.0;
}

double Process::getCpuTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return (double)cpu_time / 1000.0;
}

double Process::getTotalRunTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return (double)total_time / 1000.0;
}

double Process::getRemainingTime() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return (double)remain_time / 1000.0;
}

uint32_t Process::getRemainingCpuTimeMs() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return remain_time;
}

bool Process::hasMoreBursts() const
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    return (current_burst + 1) < num_bursts;
}

void Process::setBurstStartTime(uint64_t current_time)
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    burst_start_time = current_time;
}

void Process::setInReadyQueue(bool in_queue)
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    in_ready_queue = in_queue;
}

void Process::setState(State new_state, uint64_t current_time)
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    if (state == State::NotStarted && new_state == State::Ready)
    {
        launch_time = current_time;
    }
    state = new_state;
    last_update_time = current_time;
    if (new_state != State::Ready)
    {
        in_ready_queue = false;
    }
    if (new_state == State::Running || new_state == State::IO)
    {
        burst_start_time = current_time;
    }
    if (new_state == State::Terminated)
    {
        completion_time = current_time;
        core = -1;
    }
}

void Process::setCpuCore(int8_t core_num)
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    core = core_num;
}

void Process::interrupt()
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    is_interrupted = true;
}

void Process::interruptHandled()
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    is_interrupted = false;
}

void Process::advanceBurst()
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    if ((current_burst + 1) < num_bursts)
    {
        current_burst++;
    }
    else
    {
        current_burst = num_bursts;
    }
}

void Process::updateProcess(uint64_t current_time)
{
    // use `current_time` to update turnaround time, wait time, burst times, 
    // cpu time, and remaining time
    std::lock_guard<std::mutex> lock(proc_mutex);

    if (state == State::NotStarted || state == State::Terminated)
    {
        return;
    }

    if (last_update_time == 0)
    {
        last_update_time = current_time;
        return;
    }

    if (current_time <= last_update_time)
    {
        return;
    }

    uint64_t delta = current_time - last_update_time;
    uint64_t accounted = delta;
    if (state == State::Ready)
    {
        if (in_ready_queue)
        {
            wait_time += accounted;
        }
        turn_time += accounted;
        last_update_time = current_time;
        return;
    }

    if (current_burst >= num_bursts)
    {
        return;
    }

    accounted = std::min<uint64_t>(delta, burst_times[current_burst]);
    burst_times[current_burst] -= accounted;
    turn_time += accounted;
    if (state == State::Running)
    {
        cpu_time += accounted;
        remain_time -= accounted;
    }
    last_update_time += accounted;
}

bool Process::updateProcessIfState(State expected_state, uint64_t current_time)
{
    std::lock_guard<std::mutex> lock(proc_mutex);

    if (state != expected_state || state == State::NotStarted || state == State::Terminated)
    {
        return false;
    }

    if (last_update_time == 0)
    {
        last_update_time = current_time;
        return true;
    }

    if (current_time <= last_update_time)
    {
        return true;
    }

    uint64_t delta = current_time - last_update_time;
    uint64_t accounted = delta;
    if (state == State::Ready)
    {
        if (in_ready_queue)
        {
            wait_time += accounted;
        }
        turn_time += accounted;
        last_update_time = current_time;
        return true;
    }

    if (current_burst >= num_bursts)
    {
        return true;
    }

    accounted = std::min<uint64_t>(delta, burst_times[current_burst]);
    burst_times[current_burst] -= accounted;
    turn_time += accounted;
    if (state == State::Running)
    {
        cpu_time += accounted;
        remain_time -= accounted;
    }
    last_update_time += accounted;
    return true;
}

void Process::updateBurstTime(int burst_idx, uint32_t new_time)
{
    std::lock_guard<std::mutex> lock(proc_mutex);
    burst_times[burst_idx] = new_time;
}
