variable "digitalocean_access_token" {
  type      = string
  sensitive = true
}

variable "app_name" {
  type    = string
  default = "async-job-pipeline"

  validation {
    condition     = length(var.app_name) <= 32
    error_message = "DigitalOcean App Platform app names must be at most 32 characters."
  }
}

variable "region" {
  type    = string
  default = "nyc1"
}

variable "vpc_ip_range" {
  description = "Private CIDR range for App Platform to self-managed infrastructure communication."
  type        = string
  default     = "10.30.0.0/16"
}

variable "github_repo" {
  type        = string
  description = "GitHub repo in owner/name form."
}

variable "github_branch" {
  type    = string
  default = "main"
}

variable "postgres_database" {
  type    = string
  default = "jobs"
}

variable "postgres_user" {
  type    = string
  default = "jobs_user"
}

variable "infra_droplet_size" {
  type    = string
  default = "s-1vcpu-2gb"
}

variable "infra_droplet_image" {
  type    = string
  default = "ubuntu-24-04-x64"
}

variable "ssh_allowed_cidrs" {
  type    = list(string)
  default = []
}

variable "api_instance_count" {
  type    = number
  default = 1
}

variable "worker_instance_count" {
  type    = number
  default = 1
}

variable "api_instance_size" {
  type    = string
  default = "basic-xxs"
}

variable "worker_instance_size" {
  type    = string
  default = "basic-xxs"
}

variable "max_page_size" {
  type    = string
  default = "100"
}

variable "max_payload_bytes" {
  type    = string
  default = "65536"
}

variable "worker_batch_size" {
  type    = string
  default = "10"
}

variable "worker_poll_interval_seconds" {
  type    = string
  default = "1"
}

variable "stale_lock_seconds" {
  type    = string
  default = "300"
}

variable "kafka_submitted_high_topic" {
  type    = string
  default = "jobs.submitted.high"
}

variable "kafka_submitted_default_topic" {
  type    = string
  default = "jobs.submitted.default"
}

variable "kafka_submitted_low_topic" {
  type    = string
  default = "jobs.submitted.low"
}

variable "kafka_retry_topic" {
  type    = string
  default = "jobs.retry"
}

variable "kafka_dead_letter_topic" {
  type    = string
  default = "jobs.dead_lettered"
}

variable "kafka_partition_count" {
  type    = number
  default = 3
}

variable "max_kafka_heap_mb" {
  type    = number
  default = 512
}
