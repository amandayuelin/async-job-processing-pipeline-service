locals {
  kafka_topics = {
    KAFKA_SUBMITTED_HIGH_TOPIC    = "jobs.submitted.high"
    KAFKA_SUBMITTED_DEFAULT_TOPIC = "jobs.submitted.default"
    KAFKA_SUBMITTED_LOW_TOPIC     = "jobs.submitted.low"
    KAFKA_RETRY_TOPIC             = "jobs.retry"
    KAFKA_DEAD_LETTER_TOPIC       = "jobs.dead_lettered"
  }

  common_envs = merge(
    {
      ENVIRONMENT               = "production"
      LOG_LEVEL                 = "INFO"
      MAX_PAGE_SIZE             = var.max_page_size
      MAX_PAYLOAD_BYTES         = var.max_payload_bytes
      KAFKA_BOOTSTRAP_SERVERS   = var.kafka_bootstrap_servers
    },
    local.kafka_topics
  )
}

resource "digitalocean_app" "service" {
  spec {
    name   = var.app_name
    region = var.region

    service {
      name               = "api"
      dockerfile_path    = "Dockerfile"
      run_command        = "uvicorn app.main:app --host 0.0.0.0 --port 8000"
      http_port          = 8000
      instance_count     = var.api_instance_count
      instance_size_slug = var.api_instance_size

      github {
        repo           = var.github_repo
        branch         = var.github_branch
        deploy_on_push = true
      }

      routes {
        path = "/"
      }

      health_check {
        http_path = "/healthz"
      }

      env {
        key   = "DATABASE_URL"
        value = var.database_url
        type  = "SECRET"
      }

      dynamic "env" {
        for_each = local.common_envs
        content {
          key   = env.key
          value = env.value
          type  = env.key == "KAFKA_BOOTSTRAP_SERVERS" ? "SECRET" : "GENERAL"
        }
      }

      env {
        key   = "PORT"
        value = "8000"
        type  = "GENERAL"
      }
    }

    worker {
      name               = "worker"
      dockerfile_path    = "Dockerfile"
      run_command        = "python -m app.worker"
      instance_count     = var.worker_instance_count
      instance_size_slug = var.worker_instance_size

      github {
        repo           = var.github_repo
        branch         = var.github_branch
        deploy_on_push = true
      }

      env {
        key   = "DATABASE_URL"
        value = var.database_url
        type  = "SECRET"
      }

      dynamic "env" {
        for_each = local.common_envs
        content {
          key   = env.key
          value = env.value
          type  = env.key == "KAFKA_BOOTSTRAP_SERVERS" ? "SECRET" : "GENERAL"
        }
      }

      env {
        key   = "WORKER_ID"
        value = "worker-do"
        type  = "GENERAL"
      }

      env {
        key   = "WORKER_BATCH_SIZE"
        value = var.worker_batch_size
        type  = "GENERAL"
      }

      env {
        key   = "WORKER_POLL_INTERVAL_SECONDS"
        value = var.worker_poll_interval_seconds
        type  = "GENERAL"
      }

      env {
        key   = "STALE_LOCK_SECONDS"
        value = var.stale_lock_seconds
        type  = "GENERAL"
      }
    }
  }
}
