# Optional MongoDB Atlas M10 cluster — independent module.
#
# Modeled after reference/.../mongodbatlas_advanced_cluster/m10-replicaset.
# Persists across `uv run destroy` by default; opt in via --include-cluster.

locals {
  atlas_region = upper(replace(var.cloud_region, "-", "_"))

  # Atlas rejects tags with blank values (HTTP 400 TAG_VALUE_BLANK).
  # Drop any tag whose value is empty/null so the call succeeds when the
  # caller didn't set owner_email or other optional tags.
  _raw_tags = {
    owner = var.owner_email
    app   = "streaming-agents"
  }
  cluster_tags = {
    for k, v in local._raw_tags : k => v
    if v != null && v != ""
  }
}

resource "mongodbatlas_advanced_cluster" "cluster" {
  project_id   = var.atlas_project_id
  name         = var.atlas_cluster_name
  cluster_type = "REPLICASET"

  backup_enabled                 = true
  termination_protection_enabled = false

  replication_specs = [
    {
      region_configs = [
        {
          electable_specs = {
            instance_size = "M10"
            node_count    = 3
            disk_size_gb  = 10
          }
          auto_scaling = {
            compute_enabled            = true
            compute_scale_down_enabled = true
            compute_min_instance_size  = "M10"
            compute_max_instance_size  = "M50"
            disk_gb_enabled            = true
          }
          provider_name = "AWS"
          priority      = 7
          region_name   = local.atlas_region
        }
      ]
    }
  ]

  tags = local.cluster_tags

  # Fail fast when Atlas is wedged. M10 normally provisions in 7-15 min;
  # 30 min create cap surfaces stuck states sooner than the 3h default.
  timeouts = {
    create = "30m"
    update = "30m"
    delete = "10m"
  }
}

resource "mongodbatlas_database_user" "app_user" {
  project_id         = var.atlas_project_id
  username           = var.atlas_db_username
  password           = var.atlas_db_password
  auth_database_name = "admin"

  roles {
    role_name     = "atlasAdmin"
    database_name = "admin"
  }

  depends_on = [mongodbatlas_advanced_cluster.cluster]
}

resource "mongodbatlas_project_ip_access_list" "workshop" {
  # Parameterized CIDR. Workshop mode passes ["0.0.0.0/0"]
  # (legacy default). Non-workshop deploys pass the deployer's egress IP
  # as a /32. The for_each accepts a set (via toset) because the Atlas
  # provider requires one access-list resource per CIDR.
  for_each   = toset(var.atlas_access_cidrs)
  project_id = var.atlas_project_id
  cidr_block = each.value
  comment    = "streaming-agents (${each.value})"
}

# Migrate the legacy single-instance `.workshop` resource
# address into the new for_each-keyed form. Without this, terraform
# sees the un-keyed resource as removed and re-adds it under a new
# address, briefly leaving the Atlas access list empty mid-apply.
#
# Terraform requires `moved.to` to be a static address — only attribute
# access and indexing with CONSTANT keys are allowed. We therefore
# pin the target to "0.0.0.0/0", which is the ONLY CIDR earlier
# deploys ever used (workshop default).
#
# The workshop → hardened upgrade path (CIDR changes
# from "0.0.0.0/0" to a /32 egress IP) is handled by deploy.py's
# `_pre_apply_state_mv_for_atlas_cidr` which runs `terraform state mv`
# BEFORE `terraform apply` to relocate the keyed resource without
# delete+recreate. Without that helper, switching `atlas_access_cidrs`
# from ["0.0.0.0/0"] to ["<egress>/32"] reopens the mid-apply gap.
moved {
  from = mongodbatlas_project_ip_access_list.workshop
  to   = mongodbatlas_project_ip_access_list.workshop["0.0.0.0/0"]
}
