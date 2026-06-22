output "app_id" {
  value = digitalocean_app.service.id
}

output "app_live_url" {
  value = digitalocean_app.service.live_url
}

output "app_default_ingress" {
  value = digitalocean_app.service.default_ingress
}
