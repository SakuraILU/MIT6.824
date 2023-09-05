package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type JobManager struct {
	jobchan      chan *Job
	jobs         map[JobId]*Job
	lk           sync.RWMutex
	expire_time  time.Duration
	check_interv time.Duration
}

func NewJobManager(size int) (jm *JobManager) {
	jm = &JobManager{
		jobchan:      make(chan *Job, size),
		jobs:         make(map[JobId]*Job, size),
		lk:           sync.RWMutex{},
		expire_time:  10 * time.Second,
		check_interv: 2 * time.Second,
	}

	go jm.ExpireChecker()

	return
}

func (jm *JobManager) Push(j *Job) {
	jm.lk.Lock()
	defer jm.lk.Unlock()

	jm.jobchan <- j
	jm.jobs[j.Id] = j

	log.Printf("Put job (%s, %d)", j.Filename, j.Id)
}

func (jm *JobManager) Pop() (j *Job) {
	jm.lk.Lock()
	defer jm.lk.Unlock()

	select {
	case j = <-jm.jobchan:
		switch j.Type {
		case MAPREADY:
			j.Type = MAP
		case REDUCEREADY:
			j.Type = REDUCE
		}

		j.StartTime = time.Now()

		return
	default:
		return nil
	}
}

func (jm *JobManager) JobDone(job *Job) (err error) {
	jm.lk.Lock()
	defer jm.lk.Unlock()

	if _, ok := jm.jobs[job.Id]; !ok {
		return fmt.Errorf("Job with id %v is not exist", job.Id)
	}

	delete(jm.jobs, job.Id)
	return
}

func (jm *JobManager) CheckAllDone() bool {
	jm.lk.Lock()
	defer jm.lk.Unlock()

	if len(jm.jobs) > 0 {
		return false
	}

	for _, job := range jm.jobs {
		if job.Type == MAP || job.Type == REDUCE {
			return false
		} else if job.Type == DONE {
			delete(jm.jobs, job.Id)
		} else {
			log.Println("shouldn't come here! no job is in ready")
		}
	}
	return true
}

func (jm *JobManager) ExpireChecker() {
	is_expire := func(j *Job) bool {
		return time.Now().After(j.StartTime.Add(jm.expire_time))
	}

	for {
		time.Sleep(jm.check_interv)

		jm.lk.Lock()

		for _, job := range jm.jobs {
			if (job.Type == MAP || job.Type == REDUCE) && is_expire(job) {
				switch job.Type {
				case MAP:
					job.Type = MAPREADY
				case REDUCE:
					job.Type = REDUCEREADY
				}

				jm.jobchan <- job
			}
		}

		jm.lk.Unlock()
	}
}
