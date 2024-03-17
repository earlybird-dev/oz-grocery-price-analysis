variable "creds" {
  description = "The ID of the project where you want to create your resources."
  type        = string
  default     = "../gcp.json"
}

variable "project_id" {
  description = "The ID of the project where you want to create your resources."
  type        = string
  default     = "grocery-price-analysis"
}

variable "region" {
  description = "The ID of the project where you want to create your resources."
  type        = string
  default     = "australia-southeast2"
}

variable "docker_image" {
  type        = string
  description = "The docker image to deploy to Cloud Run."
  default     = "trunghadev/grocery-web-scraping:latest"
}

variable "container_cpu" {
  description = "Container cpu"
  default     = "2000m"
}

variable "container_memory" {
  description = "Container memory"
  default     = "2G"
}
