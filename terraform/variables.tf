variable "creds" {
  description = "The ID of the project where you want to create your resources."
  type        = string
  default     = "../grocery-price-analysis-d0419829e9b5.json"
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
  default     = "trunghadev/grocery-crawler:test2"
}

variable "container_cpu" {
  description = "Container cpu"
  default     = "2000m"
}

variable "container_memory" {
  description = "Container memory"
  default     = "2G"
}
