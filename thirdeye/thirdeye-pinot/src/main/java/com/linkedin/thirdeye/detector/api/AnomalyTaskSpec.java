package com.linkedin.thirdeye.detector.api;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;


import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;


/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly job, which in turn
 * spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the workers
 */
@Entity
@Table(name = "anomaly_tasks")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findAll", query = "SELECT at FROM AnomalyTaskSpec at"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobId", query = "SELECT at FROM AnomalyTaskSpec at WHERE at.jobId = :jobId"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByStatusOrderByCreateTimeAscending", query = "SELECT at FROM AnomalyTaskSpec at WHERE at.status = :status order by at.taskStartTime asc"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatus", query = "UPDATE AnomalyTaskSpec SET status = :newStatus WHERE status = :oldStatus and id = :id"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatusAndWorkerId", query = "UPDATE AnomalyTaskSpec SET status = :newStatus, workerId = :workerId WHERE status = :oldStatus and id = :id")
})
public class AnomalyTaskSpec {
  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(name = "job_id", nullable = false)
  private long jobId;

  @Enumerated(EnumType.STRING)
  @Column(name = "task_type", nullable = false)
  private TaskType taskType;

  @Column(name = "worker_id", nullable = true)
  private Long workerId;

  @Column(name = "job_name", nullable = false)
  private String jobName;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private TaskStatus status;

  @Column(name = "task_start_time", nullable = false)
  private long taskStartTime;

  @Column(name = "task_end_time", nullable = false)
  private long taskEndTime;

  @Column(name = "task_info", nullable = false)
  private String taskInfo;

  public AnomalyTaskSpec() {
  }



  public long getId() {
    return id;
  }



  public void setId(long id) {
    this.id = id;
  }



  public long getJobId() {
    return jobId;
  }



  public Long getWorkerId() {
    return workerId;
  }



  public void setWorkerId(Long workerId) {
    this.workerId = workerId;
  }



  public void setJobName(String jobName) {
    this.jobName = jobName;
  }


  public String getJobName() {
    return jobName;
  }



  public void setJobId(long jobId) {
    this.jobId = jobId;
  }



  public TaskStatus getStatus() {
    return status;
  }



  public void setStatus(TaskStatus status) {
    this.status = status;
  }



  public long getTaskStartTime() {
    return taskStartTime;
  }



  public void setTaskStartTime(long startTime) {
    this.taskStartTime = startTime;
  }



  public long getTaskEndTime() {
    return taskEndTime;
  }



  public void setTaskEndTime(long endTime) {
    this.taskEndTime = endTime;
  }



  public String getTaskInfo() {
    return taskInfo;
  }



  public void setTaskInfo(String taskInfo) {
    this.taskInfo = taskInfo;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public void setTaskType(TaskType taskType) {
    this.taskType = taskType;
  }



  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyTaskSpec)) {
      return false;
    }
    AnomalyTaskSpec af = (AnomalyTaskSpec) o;
    return Objects.equals(id, af.getId()) && Objects.equals(jobId, af.getJobId())
        && Objects.equals(status, af.getStatus()) && Objects.equals(taskStartTime, af.getTaskStartTime())
        && Objects.equals(taskEndTime, af.getTaskEndTime()) && Objects.equals(taskInfo, af.getTaskInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, jobId, status, taskStartTime, taskEndTime, taskInfo);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", id).add("jobId", jobId)
        .add("status", status).add("startTime", taskStartTime).add("endTime", taskEndTime).add("taskInfo", taskInfo).toString();
  }
}
