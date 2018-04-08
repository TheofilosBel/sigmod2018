#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H

// #include "defn.h"
#include <pthread.h>
#include <semaphore.h>
#include <iostream>
#include <signal.h>
#include <queue>
#include <sys/sysinfo.h>
#include <unistd.h>
#include "job.h"
#include "cpu_mapping.h"
#include "defn.h"

using std::queue;

// the cpu mapping from cpu mapping .cpp
//int numa[][4] = {0,1,2,3};
#ifdef MY_PC
extern int numa[][4];
#endif

#ifdef MY_PC2
extern int numa[][2];
#endif

#ifdef SIGMOD_1CPU/*<---------------------------ALWAYS DEFIEND IT BEFORE UPLOAD-------------------*/
extern int numa[][20];
#endif

#ifdef SIGMOD_2CPU
extern int numa[][20];
#endif



// Class JobScheduler
class JobScheduler {
 public:
  JobScheduler() = default;
  ~JobScheduler() = default;

  bool Init(int num_of_threads, int numa_region) {
    int i;
    num_of_executors_ = num_of_threads;
    id_gen_ = 0;
    executors_counter_ = 0;
    pthread_mutexattr_init(&jobs_mutexattr_);
    pthread_mutex_init(&jobs_mutex_, &jobs_mutexattr_);
    sem_init(&available_work_, 0, 0);

    // inti attrs for threads
    pthread_attr_t attr;
    cpu_set_t set;
    pthread_attr_init(&attr);

    if ( (executors_ = new JobExecutor*[num_of_executors_]) == nullptr ) {
      return false;
    }

    for ( i=0; i<num_of_executors_; i++){
      executors_[i] = new JobExecutor(&jobs_mutex_, &available_work_, &jobs_);
      if (executors_[i] == nullptr ) {
        return false;
      }


      // Bind thread to physical thread
      CPU_ZERO(&set);
      CPU_SET(numa[numa_region][i], &set);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

      // Create bounded threads
      executors_[i]->Create(&attr, numa_region);
    }

    return true;
  }

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

  JobID Schedule(Job* job) {
    JobID id = ++id_gen_;

    pthread_mutex_lock(&jobs_mutex_);
    jobs_.push(job);
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

private:
  // Class BarrierJob    -
  class BarrierJob : public Job {
   public:

    BarrierJob(pthread_barrier_t *barrier) : barrier_(barrier){}
    virtual ~BarrierJob() = default;
    int Run() {
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
    JobExecutor(pthread_mutex_t *jobs_mutex, sem_t *avail_jobs, queue<Job*> *jobs)
        : jobs_mutex_(jobs_mutex),
          avail_jobs_(avail_jobs),
          jobs_(jobs),
          to_exit_(false) {}

    virtual ~JobExecutor() {}

    bool Create(pthread_attr_t * attr, int numa) {
      int r = pthread_create(&thread_id_, attr, CallThrFn, this);
      return !r;
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
    // thread function
    static void* CallThrFn(void *pthis) {

      JobExecutor *executor = static_cast<JobExecutor*>(pthis);
      Job *job = nullptr;

      // untill stop/kill
      while(!executor->to_exit_) {
        // wait for a job
        sem_wait(executor->avail_jobs_);
        // lock fifo
        pthread_mutex_lock(executor->jobs_mutex_);

        // if there is no job
        if ( !executor->jobs_->size() ) {
          // unlock and exit
          pthread_mutex_unlock(executor->jobs_mutex_);
          pthread_exit((void*) NULL);
        }

        // else pop and unlock
        job = executor->jobs_->front();
        executor->jobs_->pop();
        pthread_mutex_unlock(executor->jobs_mutex_);

        job->Run();
        delete job;
      }

      pthread_exit((void*) NULL);
    }

    pthread_mutex_t *jobs_mutex_;
    sem_t *avail_jobs_;
    queue<Job*> *jobs_;

    volatile bool to_exit_;
    pthread_t thread_id_;
  };

  JobID id_gen_;
  int num_of_executors_, executors_counter_;
  pthread_mutexattr_t jobs_mutexattr_;
  pthread_mutex_t jobs_mutex_;
  pthread_barrier_t barrier_;
  sem_t available_work_;
  queue<Job*> jobs_;
  JobExecutor **executors_;

  DISALLOW_COPY_AND_ASSIGN(JobScheduler);
};

#endif    // JOB_SCHEDULER_H
