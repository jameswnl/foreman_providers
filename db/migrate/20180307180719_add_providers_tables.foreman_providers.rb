class AddProvidersTables < ActiveRecord::Migration[5.1]
  def up
    create_table "providers_authentications", id: :bigserial, force: :cascade do |t|
      t.string   "name"
      t.string   "authtype"
      t.string   "userid"
      t.string   "password"
      t.bigint   "resource_id"
      t.string   "resource_type"
      t.datetime "created_on"
      t.datetime "updated_on"
      t.datetime "last_valid_on"
      t.datetime "last_invalid_on"
      t.datetime "credentials_changed_on"
      t.string   "status"
      t.string   "status_details"
      t.string   "type"
      t.text     "auth_key"
      t.string   "fingerprint"
      t.string   "service_account"
      t.boolean  "challenge"
      t.boolean  "login"
      t.text     "public_key"
      t.text     "htpassd_users",                             default: [], array: true
      t.text     "ldap_id",                                   default: [], array: true
      t.text     "ldap_email",                                default: [], array: true
      t.text     "ldap_name",                                 default: [], array: true
      t.text     "ldap_preferred_user_name",                  default: [], array: true
      t.string   "ldap_bind_dn"
      t.boolean  "ldap_insecure"
      t.string   "ldap_url"
      t.string   "request_header_challenge_url"
      t.string   "request_header_login_url"
      t.text     "request_header_headers",                    default: [], array: true
      t.text     "request_header_preferred_username_headers", default: [], array: true
      t.text     "request_header_name_headers",               default: [], array: true
      t.text     "request_header_email_headers",              default: [], array: true
      t.string   "open_id_sub_claim"
      t.string   "open_id_user_info"
      t.string   "open_id_authorization_endpoint"
      t.string   "open_id_token_endpoint"
      t.text     "open_id_extra_scopes",                      default: [], array: true
      t.text     "open_id_extra_authorize_parameters"
      t.text     "certificate_authority"
      t.string   "google_hosted_domain"
      t.text     "github_organizations",                      default: [], array: true
      t.string   "rhsm_sku"
      t.string   "rhsm_pool_id"
      t.string   "rhsm_server"
      t.string   "manager_ref"
      t.text     "options"
      t.index ["resource_id", "resource_type"], name: "index_authentications_on_resource_id_and_resource_type", using: :btree
    end

    create_table "providers_endpoints", id: :bigserial, force: :cascade do |t|
      t.string   "role"
      t.string   "ipaddress"
      t.string   "hostname"
      t.integer  "port"
      t.string   "resource_type"
      t.bigint   "resource_id"
      t.datetime "created_at",            null: false
      t.datetime "updated_at",            null: false
      t.integer  "verify_ssl"
      t.string   "url"
      t.string   "security_protocol"
      t.string   "api_version"
      t.string   "path"
      t.text     "certificate_authority"
    end

    create_table "providers_ext_management_systems" do |t|
      t.string   "name"
      t.datetime "created_on"
      t.datetime "updated_on"
      t.string   "guid"
      t.bigint   "zone_id"
      t.string   "type"
      t.string   "api_version"
      t.string   "uid_ems"
      t.integer  "host_default_vnc_port_start"
      t.integer  "host_default_vnc_port_end"
      t.string   "provider_region"
      t.text     "last_refresh_error"
      t.datetime "last_refresh_date"
      t.bigint   "provider_id"
      t.string   "realm"
      t.bigint   "tenant_id"
      t.string   "project"
      t.bigint   "parent_ems_id"
      t.string   "subscription"
      t.text     "last_metrics_error"
      t.datetime "last_metrics_update_date"
      t.datetime "last_metrics_success_date"
      t.boolean  "tenant_mapping_enabled"
      t.boolean  "enabled"
      t.text     "options"
      t.index ["guid"], name: "index_ext_management_systems_on_guid", unique: true, using: :btree
      t.index ["parent_ems_id"], name: "index_ext_management_systems_on_parent_ems_id", using: :btree
    end
  end

  def down
    drop_table "providers_authentications"
    drop_table "providers_endpoints"
    drop_table "providers_ext_management_systems"
  end
end
