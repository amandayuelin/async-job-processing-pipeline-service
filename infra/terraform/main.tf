resource "random_password" "postgres" {
  length  = 24
  special = false
}

resource "random_id" "kafka_cluster" {
  byte_length = 16
}

resource "digitalocean_vpc" "main" {
  name     = "${var.app_name}-vpc"
  region   = var.region
  ip_range = var.vpc_ip_range
}

resource "digitalocean_droplet" "infra" {
  name     = "${var.app_name}-infra"
  region   = var.region
  size     = var.infra_droplet_size
  image    = var.infra_droplet_image
  vpc_uuid = digitalocean_vpc.main.id
  tags     = ["${var.app_name}", "self-managed-infra"]
  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    postgres_database             = var.postgres_database
    postgres_user                 = var.postgres_user
    postgres_password             = random_password.postgres.result
    kafka_cluster_id              = random_id.kafka_cluster.b64_url
    kafka_submitted_high_topic    = var.kafka_submitted_high_topic
    kafka_submitted_default_topic = var.kafka_submitted_default_topic
    kafka_submitted_low_topic     = var.kafka_submitted_low_topic
    kafka_retry_topic             = var.kafka_retry_topic
    kafka_dead_letter_topic       = var.kafka_dead_letter_topic
    kafka_partition_count         = var.kafka_partition_count
    max_kafka_heap_mb             = var.max_kafka_heap_mb
  })
}

resource "digitalocean_firewall" "infra" {
  name        = "${var.app_name}-infra"
  droplet_ids = [digitalocean_droplet.infra.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "5432"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "9092"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  dynamic "inbound_rule" {
    for_each = var.ssh_allowed_cidrs
    content {
      protocol         = "tcp"
      port_range       = "22"
      source_addresses = [inbound_rule.value]
    }
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "all"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "all"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
