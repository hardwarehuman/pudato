variable "prefix" {
  description = "Prefix for all resource names"
  type        = string
  default     = "pudato"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
