#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <list>
#include <string>
#include <thread>
#include <vector>
#include <ncurses.h>
#include "configreader.h"
#include "process.h"

typedef struct ReadyEvent {
    Process *process;
    uint64_t event_time;
    bool from_io;
} ReadyEvent;

typedef struct SchedulerStats {
    double cpu_utilization;
    double first_half_throughput;
    double second_half_throughput;
    double overall_throughput;
    double average_turnaround;
    double average_waiting;
} SchedulerStats;

// Shared data for all cores
typedef struct SchedulerData {
    std::mutex queue_mutex;
    ScheduleAlgorithm algorithm;
    uint32_t context_switch;
    uint32_t time_slice;
    std::list<Process*> ready_queue;
    std::vector<bool> core_busy;
    std::vector<Process*> running_processes;
    std::atomic<bool> all_terminated;
    std::atomic<uint64_t> context_switch_busy_ms;
} SchedulerData;

void coreRunProcesses(uint8_t core_id, SchedulerData *data);
void enqueueReadyProcess(Process *process, SchedulerData *data);
void maybePreemptForPriority(Process *ready_process, SchedulerData *data);
void printProcessOutput(const std::vector<Process*>& processes);
SchedulerStats calculateStatistics(const std::vector<Process*>& processes, uint8_t num_cores,
                                   uint64_t simulation_start, uint64_t simulation_end,
                                   uint64_t context_switch_busy_ms);
void printStatisticsNcurses(const SchedulerStats& stats);
void printStatisticsStdout(const SchedulerStats& stats);
std::string makeProgressString(double percent, uint32_t width);
double calculateThroughput(std::size_t completed_count, uint64_t start_time, uint64_t end_time);
uint64_t currentTime();
std::string processStateToString(Process::State state);

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Error: must specify configuration file" << std::endl;
        return EXIT_FAILURE;
    }

    SchedulerConfig *config = scr::readConfigFile(argv[1]);
    SchedulerData *shared_data = new SchedulerData();
    std::vector<Process*> processes;

    shared_data->algorithm = config->algorithm;
    shared_data->context_switch = config->context_switch;
    shared_data->time_slice = config->time_slice;
    shared_data->all_terminated = false;
    shared_data->context_switch_busy_ms = 0;
    shared_data->core_busy.assign(config->cores, false);
    shared_data->running_processes.assign(config->cores, NULL);

    uint64_t simulation_start = currentTime();
    for (int i = 0; i < config->num_processes; i++)
    {
        Process *process = new Process(config->processes[i], simulation_start);
        processes.push_back(process);
        if (process->getState() == Process::State::Ready)
        {
            enqueueReadyProcess(process, shared_data);
        }
    }
    scr::deleteConfig(config);

    std::vector<std::thread> schedule_threads;
    for (std::size_t i = 0; i < shared_data->running_processes.size(); i++)
    {
        schedule_threads.push_back(std::thread(coreRunProcesses,
                                               static_cast<uint8_t>(i), shared_data));
    }

    initscr();
    uint64_t last_refresh_time = 0;
    while (!(shared_data->all_terminated.load()))
    {
        uint64_t now = currentTime();
        std::vector<ReadyEvent> ready_events;

        for (Process *process : processes)
        {
            Process::State state = process->getState();
            if (state == Process::State::NotStarted)
            {
                uint64_t ready_time = simulation_start + process->getStartTime();
                if (now >= ready_time)
                {
                    ready_events.push_back({process, ready_time, false});
                }
            }
            else if (state == Process::State::IO)
            {
                uint64_t io_complete_time = process->getLastUpdateTime() +
                                            process->getCurrentBurstRemainingMs();
                if (process->getCurrentBurstRemainingMs() == 0 || now >= io_complete_time)
                {
                    ready_events.push_back({process, io_complete_time, true});
                }
                else
                {
                    process->updateProcess(now);
                }
            }
            else if (state == Process::State::Ready)
            {
                process->updateProcessIfState(Process::State::Ready, now);
            }
        }

        std::stable_sort(ready_events.begin(), ready_events.end(),
            [](const ReadyEvent& lhs, const ReadyEvent& rhs) {
                return lhs.event_time < rhs.event_time;
            });

        for (const ReadyEvent& event : ready_events)
        {
            Process *process = event.process;
            if (event.from_io)
            {
                if (process->getState() != Process::State::IO)
                {
                    continue;
                }
                process->updateProcess(event.event_time);
                if (process->getCurrentBurstRemainingMs() != 0)
                {
                    continue;
                }
                process->advanceBurst();
            }
            else if (process->getState() != Process::State::NotStarted)
            {
                continue;
            }

            process->setCpuCore(-1);
            process->setState(Process::State::Ready, event.event_time);
            process->setInReadyQueue(true);
            process->updateProcess(now);
            enqueueReadyProcess(process, shared_data);
            maybePreemptForPriority(process, shared_data);
        }

        bool all_terminated = true;
        for (Process *process : processes)
        {
            if (process->getState() != Process::State::Terminated)
            {
                all_terminated = false;
                break;
            }
        }
        shared_data->all_terminated.store(all_terminated);

        if (last_refresh_time == 0 || (now - last_refresh_time) >= 50 || all_terminated)
        {
            erase();
            printProcessOutput(processes);
            last_refresh_time = now;
        }

        if (!all_terminated)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    for (std::thread& thread : schedule_threads)
    {
        thread.join();
    }

    uint64_t simulation_end = currentTime();
    erase();
    printProcessOutput(processes);
    SchedulerStats stats = calculateStatistics(processes,
                                               shared_data->running_processes.size(),
                                               simulation_start, simulation_end,
                                               shared_data->context_switch_busy_ms.load());
    printStatisticsNcurses(stats);
    refresh();

    for (Process *process : processes)
    {
        delete process;
    }
    endwin();
    printStatisticsStdout(stats);
    delete shared_data;

    return 0;
}

void coreRunProcesses(uint8_t core_id, SchedulerData *shared_data)
{
    while (!(shared_data->all_terminated.load()))
    {
        Process *process = NULL;
        {
            std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
            if (!(shared_data->ready_queue.empty()))
            {
                process = shared_data->ready_queue.front();
                shared_data->ready_queue.pop_front();
                shared_data->core_busy[core_id] = true;
            }
            else
            {
                shared_data->core_busy[core_id] = false;
            }
        }

        if (process == NULL)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        uint64_t dequeue_time = currentTime();
        process->updateProcessIfState(Process::State::Ready, dequeue_time);
        process->setInReadyQueue(false);
        std::this_thread::sleep_for(std::chrono::milliseconds(shared_data->context_switch));
        shared_data->context_switch_busy_ms.fetch_add(shared_data->context_switch);

        uint64_t run_start = currentTime();
        process->setCpuCore(core_id);
        process->setState(Process::State::Running, run_start);
        {
            std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
            shared_data->running_processes[core_id] = process;
        }

        bool cpu_burst_finished = false;
        bool was_interrupted = false;
        while (!(shared_data->all_terminated.load()))
        {
            if (process->getCurrentBurstRemainingMs() == 0)
            {
                cpu_burst_finished = true;
                break;
            }

            uint64_t now = currentTime();
            uint64_t burst_end_time = process->getLastUpdateTime() +
                                      process->getCurrentBurstRemainingMs();
            uint64_t update_time = now;
            uint32_t sleep_time = std::min<uint32_t>(5, process->getCurrentBurstRemainingMs());

            if (shared_data->algorithm == ScheduleAlgorithm::RR)
            {
                uint64_t slice_end_time = run_start + shared_data->time_slice;
                if (update_time > slice_end_time)
                {
                    update_time = slice_end_time;
                }
                uint64_t slice_remaining = (slice_end_time > now) ? (slice_end_time - now) : 0;
                if (slice_remaining > 0)
                {
                    sleep_time = std::min<uint32_t>(sleep_time, slice_remaining);
                }
            }

            if (update_time > burst_end_time)
            {
                update_time = burst_end_time;
            }

            if (process->isInterrupted())
            {
                process->updateProcess(update_time);
                was_interrupted = (process->getCurrentBurstRemainingMs() > 0);
                cpu_burst_finished = !was_interrupted;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(std::max<uint32_t>(1, sleep_time)));

            now = currentTime();
            burst_end_time = process->getLastUpdateTime() + process->getCurrentBurstRemainingMs();
            update_time = std::min<uint64_t>(now, burst_end_time);
            if (shared_data->algorithm == ScheduleAlgorithm::RR)
            {
                update_time = std::min<uint64_t>(update_time, run_start + shared_data->time_slice);
            }

            process->updateProcess(update_time);

            if (process->getCurrentBurstRemainingMs() == 0)
            {
                cpu_burst_finished = true;
                break;
            }
            if (process->isInterrupted())
            {
                was_interrupted = true;
                break;
            }
            if (shared_data->algorithm == ScheduleAlgorithm::RR &&
                update_time >= (run_start + shared_data->time_slice))
            {
                was_interrupted = true;
                break;
            }
        }

        uint64_t state_change_time = process->getLastUpdateTime();
        {
            std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
            shared_data->running_processes[core_id] = NULL;
        }
        process->setCpuCore(-1);

        if (cpu_burst_finished)
        {
            process->interruptHandled();
            if (process->hasMoreBursts())
            {
                process->advanceBurst();
                process->setState(Process::State::IO, state_change_time);
            }
            else
            {
                process->setState(Process::State::Terminated, state_change_time);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(shared_data->context_switch));
            shared_data->context_switch_busy_ms.fetch_add(shared_data->context_switch);
            {
                std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
                shared_data->core_busy[core_id] = false;
            }
            continue;
        }

        if (was_interrupted)
        {
            process->setState(Process::State::Ready, state_change_time);
            process->interruptHandled();
            process->setInReadyQueue(false);
            std::this_thread::sleep_for(std::chrono::milliseconds(shared_data->context_switch));
            shared_data->context_switch_busy_ms.fetch_add(shared_data->context_switch);
            process->setState(Process::State::Ready, currentTime());
            enqueueReadyProcess(process, shared_data);
            {
                std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
                shared_data->core_busy[core_id] = false;
            }
        }
    }
}

void enqueueReadyProcess(Process *process, SchedulerData *shared_data)
{
    std::lock_guard<std::mutex> lock(shared_data->queue_mutex);

    process->setInReadyQueue(true);
    if (shared_data->algorithm == ScheduleAlgorithm::FCFS ||
        shared_data->algorithm == ScheduleAlgorithm::RR)
    {
        shared_data->ready_queue.push_back(process);
        return;
    }

    std::list<Process*>::iterator insert_position = shared_data->ready_queue.end();
    for (std::list<Process*>::iterator it = shared_data->ready_queue.begin();
         it != shared_data->ready_queue.end(); ++it)
    {
        if (shared_data->algorithm == ScheduleAlgorithm::SJF)
        {
            if (process->getRemainingCpuTimeMs() < (*it)->getRemainingCpuTimeMs())
            {
                insert_position = it;
                break;
            }
        }
        else if (shared_data->algorithm == ScheduleAlgorithm::PP)
        {
            if (process->getPriority() < (*it)->getPriority())
            {
                insert_position = it;
                break;
            }
        }
    }

    shared_data->ready_queue.insert(insert_position, process);
}

void maybePreemptForPriority(Process *ready_process, SchedulerData *shared_data)
{
    if (shared_data->algorithm != ScheduleAlgorithm::PP ||
        ready_process->getState() != Process::State::Ready)
    {
        return;
    }

    std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
    for (bool core_busy : shared_data->core_busy)
    {
        if (!core_busy)
        {
            return;
        }
    }

    Process *candidate = NULL;
    for (Process *running_process : shared_data->running_processes)
    {
        if (running_process == NULL || running_process->isInterrupted())
        {
            continue;
        }
        if (running_process->getPriority() <= ready_process->getPriority())
        {
            continue;
        }
        if (candidate == NULL || running_process->getPriority() > candidate->getPriority())
        {
            candidate = running_process;
        }
    }

    if (candidate != NULL)
    {
        candidate->interrupt();
    }
}

void printProcessOutput(const std::vector<Process*>& processes)
{
    printw("|   PID | Priority |    State    | Core |               Progress               |\n");
    printw("+-------+----------+-------------+------+--------------------------------------+\n");
    for (Process *process : processes)
    {
        if (process->getState() != Process::State::NotStarted)
        {
            uint16_t pid = process->getPid();
            uint8_t priority = process->getPriority();
            std::string process_state = processStateToString(process->getState());
            int8_t core = process->getCpuCore();
            std::string cpu_core = (core >= 0) ? std::to_string(core) : "--";
            double total_time = process->getTotalRunTime();
            double completed_time = total_time - process->getRemainingTime();
            double percent = (total_time > 0.0) ? (completed_time / total_time) : 1.0;
            std::string progress = makeProgressString(percent, 36);
            printw("| %5u | %8u | %11s | %4s | %36s |\n", pid, priority,
                   process_state.c_str(), cpu_core.c_str(), progress.c_str());
        }
    }
    refresh();
}

SchedulerStats calculateStatistics(const std::vector<Process*>& processes, uint8_t num_cores,
                                   uint64_t simulation_start, uint64_t simulation_end,
                                   uint64_t context_switch_busy_ms)
{
    SchedulerStats stats = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    if (processes.empty())
    {
        return stats;
    }

    double total_cpu_time = 0.0;
    double total_turnaround = 0.0;
    double total_wait = 0.0;
    std::vector<uint64_t> completion_times;
    completion_times.reserve(processes.size());

    for (Process *process : processes)
    {
        total_cpu_time += process->getCpuTime();
        total_turnaround += process->getTurnaroundTime();
        total_wait += process->getWaitTime();
        completion_times.push_back(process->getCompletionTimeMs());
    }

    std::sort(completion_times.begin(), completion_times.end());
    uint64_t last_completion = completion_times.back();
    double total_elapsed_seconds = (double)(simulation_end - simulation_start) / 1000.0;
    if (total_elapsed_seconds > 0.0)
    {
        double total_busy_time = total_cpu_time + ((double)context_switch_busy_ms / 1000.0);
        stats.cpu_utilization = (total_busy_time /
                                ((double)num_cores * total_elapsed_seconds)) * 100.0;
    }

    std::size_t first_half_count = processes.size() / 2;
    std::size_t second_half_count = processes.size() - first_half_count;
    if (first_half_count > 0)
    {
        stats.first_half_throughput = calculateThroughput(first_half_count, simulation_start,
                                                          completion_times[first_half_count - 1]);
    }
    uint64_t second_half_start = (first_half_count > 0) ?
                                 completion_times[first_half_count - 1] : simulation_start;
    stats.second_half_throughput = calculateThroughput(second_half_count, second_half_start,
                                                       last_completion);
    stats.overall_throughput = calculateThroughput(processes.size(), simulation_start,
                                                   last_completion);
    stats.average_turnaround = total_turnaround / processes.size();
    stats.average_waiting = total_wait / processes.size();

    return stats;
}

void printStatisticsNcurses(const SchedulerStats& stats)
{
    printw("\nCPU utilization: %.2f%%\n", stats.cpu_utilization);
    printw("Throughput average for first 50%% of processes finished: %.2f processes/sec\n",
           stats.first_half_throughput);
    printw("Throughput average for second 50%% of processes finished: %.2f processes/sec\n",
           stats.second_half_throughput);
    printw("Throughput overall average: %.2f processes/sec\n", stats.overall_throughput);
    printw("Average turnaround time: %.2f sec\n", stats.average_turnaround);
    printw("Average waiting time: %.2f sec\n", stats.average_waiting);
}

void printStatisticsStdout(const SchedulerStats& stats)
{
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "CPU utilization: " << stats.cpu_utilization << "%\n";
    std::cout << "Throughput average for first 50% of processes finished: "
              << stats.first_half_throughput << " processes/sec\n";
    std::cout << "Throughput average for second 50% of processes finished: "
              << stats.second_half_throughput << " processes/sec\n";
    std::cout << "Throughput overall average: " << stats.overall_throughput
              << " processes/sec\n";
    std::cout << "Average turnaround time: " << stats.average_turnaround << " sec\n";
    std::cout << "Average waiting time: " << stats.average_waiting << " sec\n";
}

std::string makeProgressString(double percent, uint32_t width)
{
    percent = std::max(0.0, std::min(1.0, percent));
    uint32_t n_chars = percent * width;
    std::string progress_bar(n_chars, '#');
    progress_bar.resize(width, ' ');
    return progress_bar;
}

double calculateThroughput(std::size_t completed_count, uint64_t start_time, uint64_t end_time)
{
    if (completed_count == 0 || end_time <= start_time)
    {
        return 0.0;
    }
    double elapsed_seconds = (double)(end_time - start_time) / 1000.0;
    return (double)completed_count / elapsed_seconds;
}

uint64_t currentTime()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string processStateToString(Process::State state)
{
    std::string str;
    switch (state)
    {
        case Process::State::NotStarted:
            str = "not started";
            break;
        case Process::State::Ready:
            str = "ready";
            break;
        case Process::State::Running:
            str = "running";
            break;
        case Process::State::IO:
            str = "i/o";
            break;
        case Process::State::Terminated:
            str = "terminated";
            break;
        default:
            str = "unknown";
            break;
    }
    return str;
}
