locals {
  tables = { for table in var.tables : table["table_id"] => table }

  iam_to_primitive = {
    "roles/bigquery.dataOwner": "OWNER"
    "roles/bigquery.dataEditor": "EDITOR"
    "roles/bigquery.dataViewer": "READER"
  }
}

resource "google_bigquery_dataset" "dataset" {
    dataset_id = var.dataset_id
    friendly_name = var.friendly_name
    description = var.dataset_description
    default_table_expiration_ms = var.default_table_expiration_ms
    project = var.project_id
    location = var.location

    labels = var.dataset_labels
    
    delete_contents_on_destroy = var.delete_contents_on_destroy

    dynamic "access" {
      for_each = var.access
      content {
        role = lookup(local.iam_to_primitive, access.value.role, access.value.role)

        domain = lookup(access.value, "domain", null)
        group_by_email = lookup(access.value, "group_by_email", null)
        user_by_email = lookup(access.value, "user_by_email", null)
        special_group = lookup(access.value, "special_group", null)
      }
    }
}

resource "google_bigquery_table" "table" {
  for_each = local.tables
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  friendly_name = each.key
  table_id = each.key
  labels = each.value["labels"]
  schema = file(each.value["schema"])
  expiration_time = each.value["expiration_time"]
  project = var.project_id

}

resource "google_bigquery_table" "abctable" {

  dataset_id = google_bigquery_dataset.dataset.dataset_id
  friendly_name = var.abctablename
  table_id = var.abctablename
  schema = var.abcschema
  expiration_time = var.expiration_time
  project = var.project_id

}

resource "google_bigquery_table" "errortable" {

  dataset_id = google_bigquery_dataset.dataset.dataset_id
  friendly_name = var.errortablename
  table_id = var.errortablename
  schema = var.errorschema
  expiration_time = var.expiration_time
  project = var.project_id

}
