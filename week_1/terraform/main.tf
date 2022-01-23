terraform {
  required_version = ">= 1.1.4"
  backend "gcs" {
    bucket = "tf-state-prod.de-zoomcamp.bethala.net"
    prefix = "terraform/state"
  }
}

provider "google" {
    project = var.project
    region  = var.region
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake" {
    name = "${local.data_lake_bucket}_${var.project}"
    location = var.region

    storage_class = var.storage_class
    uniform_bucket_level_access = true // Use IAM policies to control access to this bucket

    versioning {
        enabled = true
    }

    lifecycle_rule {
        action {
            type = "Delete"
        }
        condition {
            age = 90
        }
    }

    force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
    dataset_id = var.BQ_DATASET
    location = var.region
}
