#ifndef JOB_SCHEDULERMASTER_H
#define JOB_SCHEDULERMASTER_H

#include <pthread.h>
#include <semaphore.h>
#include <iostream>
#include <signal.h>
#include <queue>
#include <sys/sysinfo.h>
#include <unistd.h>

#include "job_scheduler.h"
#include "cpu_mapping.h"
#include "defn.h"
#include "Joiner.hpp"
#include "job_master.h"



// Class JobSchedulerMaster
class JobSchedulerMaster {
public:
    JobSchedulerMaster() = default;
    ~JobSchedulerMaster() = default;

    bool Init(std::vector<std::string> & file_names, int num_of_threads) {

        /* Initialize js vars */
        int i;
        num_of_executors_ = num_of_threads;
        id_gen_ = 0;
        executors_counter_ = 0;

        /* Intiliaze mutexes and semaphores */
        pthread_mutexattr_init(&jobs_mutexattr_);
        pthread_mutex_init(&jobs_mutex_, &jobs_mutexattr_);
        sem_init(&available_work_, 0, 0);

        // inti attrs for threads
        pthread_attr_t attr;
        cpu_set_t set;
        pthread_attr_init(&attr);

        for (int i = 0; i < num_of_executors_; i++) {

            if ( (executors_ = new JobExecutor*[num_of_executors_]) == nullptr ) {
                return false;
            }

            // Create 1st Thread numa reg 0

            executors_[i] = new JobExecutor(&jobs_mutex_, &available_work_, &jobs_, file_names);
            if (executors_[i] == nullptr ) {
            return false;
            }

            // Bind thread to physical thread
            CPU_ZERO(&set);
            if (i % 2 == 0)
                CPU_SET(NUMA_REG1, &set);  // REG 0
            else
                CPU_SET(NUMA_REG2, &set);  // REG 1
            pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

            // Create bounded threads
            executors_[i]->Create(&attr, i % 2);  // Call the right thread function
        }

        return true;
    }

      // Desotry the JS
    bool Destroy() {
        if ( executors_ != NULL ) {
            for(int i = 0; i < num_of_executors_; i++) {
                if (executors_[i] != NULL ) {
                    delete executors_[i];
                }
            }

            delete []executors_;
            executors_ = NULL;
        }

        sem_destroy(&available_work_);
        pthread_mutex_destroy(&jobs_mutex_);
        pthread_mutexattr_destroy(&jobs_mutexattr_);

        return true;
    }

    void Barrier() {
        pthread_barrier_init(&barrier_, NULL, num_of_executors_ + 1);
        for (int i = 0; i < num_of_executors_; i++) {
            Schedule(new BarrierJob(&barrier_));
        }

        pthread_barrier_wait(&barrier_);
        pthread_barrier_destroy(&barrier_);
    }

    JobID Schedule(JobMaster* JobMaster) {
        JobID id = ++id_gen_;

        pthread_mutex_lock(&jobs_mutex_);
        jobs_.push(JobMaster);
        pthread_mutex_unlock(&jobs_mutex_);
        sem_post(&available_work_);

        return id;
    }

    void Stop(bool force) {
        for(int i = 0; i < num_of_executors_; i++ ) {
            sem_post(&available_work_);
        }

        // join/kill executors
        for(int i=0; i<num_of_executors_; i++ ) {
            if ( force ) {
                executors_[i]->Kill();
            } else {
                executors_[i]->Stop();
            }
        }
    }

    // Class BarrierJob    -
    class BarrierJob : public JobMaster {
    public:

        BarrierJob(pthread_barrier_t *barrier) : barrier_(barrier){}
        virtual ~BarrierJob() = default;


        int Run(Joiner & j) {
            pthread_barrier_wait(barrier_);
            return 0;
        }

        private:
        pthread_barrier_t *barrier_;
    };

    // JobExecutorID
    typedef unsigned int JobExecutorID;

    // Class JobExecutor -  handles thread flow
    class JobExecutor {
    public:

        // The filename to init the joiner
        std::vector<std::string> & file_names_;

        JobExecutor(pthread_mutex_t *jobs_mutex, sem_t *avail_jobs, queue<JobMaster*> *jobs, std::vector<std::string> & file_names)
        : jobs_mutex_(jobs_mutex),
          avail_jobs_(avail_jobs),
          file_names_(file_names),
          jobs_(jobs),
          to_exit_(false) {}

        virtual ~JobExecutor() {}

        bool Create(pthread_attr_t * attr, int numa) {
            if (numa == 0) {
                int r = pthread_create(&thread_id_, attr, CallThrFn_CPU1, this);
                return !r;
            }
            else {
                int r = pthread_create(&thread_id_, attr, CallThrFn_CPU2, this);
                return !r;
            }
        }

        bool Stop() {
            return !pthread_join(thread_id_, NULL);
        }

        bool Kill() {
            to_exit_ = true;
            sem_post(avail_jobs_);
            pthread_kill(thread_id_,SIGKILL);
            return !pthread_join(thread_id_,NULL);
        }

    private:

        // thread function for numa region 0
        static void* CallThrFn_CPU1(void *pthis) {

            JobExecutor *executor = static_cast<JobExecutor*>(pthis);
            JobMaster *JobMaster = nullptr;
            Joiner joiner;
            joiner.mst = 0;
            joiner.job_scheduler.Init(THREAD_NUM_1CPU, NUMA_REG1);

            // Initialize joiners with relations
            for (std::string fn: executor->file_names_) {
                joiner.addRelation(fn.c_str());
            }

            // untill stop/kill
            while(!executor->to_exit_) {
                // wait for a JobMaster
                sem_wait(executor->avail_jobs_);
                // lock fifo
                pthread_mutex_lock(executor->jobs_mutex_);

                // if there is no JobMaster
                if ( !executor->jobs_->size() ) {
                    // unlock and exit
                    pthread_mutex_unlock(executor->jobs_mutex_);
                    pthread_exit((void*) NULL);
                }

                //std::cerr << "Query About to execute by master 0" << '\n';

                // else pop and unlock
                JobMaster = executor->jobs_->front();
                executor->jobs_->pop();
                pthread_mutex_unlock(executor->jobs_mutex_);

                JobMaster->Run(joiner);

            }

            joiner.job_scheduler.Destroy();
            pthread_exit((void*) NULL);
        }
        // thread function for numa region 0
        static void* CallThrFn_CPU2(void *pthis) {

            JobExecutor *executor = static_cast<JobExecutor*>(pthis);
            JobMaster *JobMaster = nullptr;
            Joiner joiner;
            joiner.mst = 1;
            joiner.job_scheduler.Init(THREAD_NUM_1CPU, NUMA_REG2);

            // Initialize joiners with relations
            for (std::string fn: executor->file_names_) {
                joiner.addRelation(fn.c_str());
            }

            // untill stop/kill
            while(!executor->to_exit_) {
                // wait for a JobMaster
                sem_wait(executor->avail_jobs_);
                // lock fifo
                pthread_mutex_lock(executor->jobs_mutex_);

                // if there is no JobMaster
                if ( !executor->jobs_->size() ) {
                    // unlock and exit
                    pthread_mutex_unlock(executor->jobs_mutex_);
                    pthread_exit((void*) NULL);
                }

                //std::cerr << "Query About to execute by master 0" << '\n';

                // else pop and unlock
                JobMaster = executor->jobs_->front();
                executor->jobs_->pop();
                pthread_mutex_unlock(executor->jobs_mutex_);

                JobMaster->Run(joiner);

            }

            joiner.job_scheduler.Destroy();
            pthread_exit((void*) NULL);
        }

        pthread_mutex_t *jobs_mutex_;
        sem_t *avail_jobs_;
        queue<JobMaster*> *jobs_;

        volatile bool to_exit_;
        pthread_t thread_id_;

    };  // ---------- END OF EXECUTOR

    JobID id_gen_;
    int num_of_executors_, executors_counter_;
    pthread_mutexattr_t jobs_mutexattr_;
    pthread_mutex_t jobs_mutex_;
    pthread_barrier_t barrier_;
    sem_t available_work_;
    queue<JobMaster*> jobs_;
    JobExecutor **executors_;

    DISALLOW_COPY_AND_ASSIGN(JobSchedulerMaster);
};

#endif    // JOB_SCHEDULER_H
