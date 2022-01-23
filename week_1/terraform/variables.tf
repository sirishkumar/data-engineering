locals {
  data_lake_bucket = "dtc_data_lake"
}

variable terraform_tf_state_bucket {
  type = string
  default = "Bucket to save terraform state"
}

variable project {
  type        = string
  description = "GCP Project ID"
}

variable region {
  type        = string
  description = "GCP Region"
}

variable storage_class {
    type        = string
    default     = "STANDARD"
    description = "GCP bucket storage class"
}

variable "BQ_DATASET" {
    type        = string
    default     = "trips_data_all"
    description = "BigQuery Dataset to store raw data"
}