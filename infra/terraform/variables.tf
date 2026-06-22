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
  default = "nyc"
}

variable "github_repo" {
  type        = string
  description = "GitHub repo in owner/name form."
}

variable "github_branch" {
  type    = string
  default = "main"
}

variable "database_url" {
  type      = string
  sensitive = true
}

variable "kafka_bootstrap_servers" {
  type      = string
  sensitive = true
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
