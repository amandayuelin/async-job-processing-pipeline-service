output "vpc_id" {
  value = digitalocean_vpc.main.id
}

output "database_url" {
  value     = "postgresql+psycopg://${var.postgres_user}:${random_password.postgres.result}@${digitalocean_droplet.infra.ipv4_address}:5432/${var.postgres_database}"
  sensitive = true
}

output "kafka_bootstrap_servers" {
  value = "${digitalocean_droplet.infra.ipv4_address}:9092"
}

output "infra_public_ip" {
  value = digitalocean_droplet.infra.ipv4_address
}

output "infra_private_ip" {
  value = digitalocean_droplet.infra.ipv4_address_private
}
