#
# Calling order for EmsCloud
# - ems
#   - flavors
#   - availability_zones
#   - host_aggregates
#   - cloud_tenants
#   - key_pairs
#   - orchestration_templates
#   - orchestration_templates_catalog
#   - orchestration_stacks
#   - security_groups
#     - firewall_rules
#   - cloud_volumes
#   - cloud_volume_backups
#   - cloud_volume_snapshots
#   - vms
#     - storages (link)
#     - security_groups (link)
#     - operating_system
#     - hardware
#       - disks
#       - guest_devices
#     - custom_attributes
#     - snapshots
#   - cloud_object_store_containers
#     - cloud_object_store_objects
#   - cloud_services
#

module Providers
  module EmsRefresh
    module SaveInventoryCloud
      def save_ems_cloud_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?
        log_header = "EMS: [#{ems.name}], id: [#{ems.id}]"

        # Check if the data coming in reflects a complete removal from the ems
        if hashes.blank?
          target.disconnect_inv if disconnect
          return
        end

        _log.info("#{log_header} Saving EMS Inventory...")
        if debug_trace
          require 'yaml'
          _log.debug("#{log_header} hashes:\n#{YAML.dump(hashes)}")
        end

        if hashes[:tag_mapper]
          ManagerRefresh::SaveInventory.save_inventory(ems, [hashes[:tag_mapper].tags_to_resolve_collection])
        end

        child_keys = [
          :resource_groups,
          :cloud_tenants,
          :flavors,
          :availability_zones,
          :host_aggregates,
          :key_pairs,
          :orchestration_templates,
          :orchestration_templates_catalog,
          :orchestration_stacks,
          :cloud_volumes,
          :cloud_volume_backups,
          :cloud_volume_snapshots,
          :instances,
          :cloud_resource_quotas,
          :cloud_object_store_containers,
          :cloud_object_store_objects,
          :cloud_services,
        ]

        # Save and link other subsections
        save_child_inventory(ems, hashes, child_keys, target, disconnect)

        link_volumes_to_base_snapshots(hashes[:cloud_volumes]) if hashes.key?(:cloud_volumes)
        link_parents_to_cloud_tenant(hashes[:cloud_tenants]) if hashes.key?(:cloud_tenants)

        ems.save!
        hashes[:id] = ems.id

        _log.info("#{log_header} Saving EMS Inventory...Complete")

        ems
      end

      #
      # what the heck: almost duplicating save_vms_inventory
      #

      def save_instances_inventory(ems, hashes, target = nil, disconnect = true)
        return if hashes.nil?
        target = ems if target.nil? && disconnect
        log_header = "EMS: [#{ems.name}], id: [#{ems.id}]"

        disconnects = if disconnect && (target.kind_of?(ExtManagementSystem) || target.kind_of?(Cloud::AvailabilityZone))
          target.instances.reload.to_a
        elsif disconnect && target.kind_of?(Cloud::Instance)
          [target.ruby_clone]
        else
          []
        end

        child_keys       = []
        extra_infra_keys = [:hardware, :custom_attributes, :snapshots, :advanced_settings, :labels, :tags, :host, :ems_cluster, :storage, :storages, :storage_profile, :raw_power_state, :parent_vm]
        extra_cloud_keys = [
          :resource_group,
          :flavor,
          :availability_zone,
          :cloud_tenant,
          :cloud_tenants,
          :cloud_network,
          :cloud_subnet,
          :security_groups,
          :key_pairs,
          :orchestration_stack,
        ]
        remove_keys = child_keys + extra_infra_keys + extra_cloud_keys

        # Query for all of the Vms once across all EMSes, to handle any moving VMs
        vms_uids = hashes.collect { |h| h[:uid_ems] }.compact
        vms = Cloud::Instance.where(:uid_ems => vms_uids).to_a
        disconnects_index = disconnects.index_by { |vm| vm }
        vms_by_uid_ems = vms.group_by(&:uid_ems)
        dup_vms_uids = (vms_uids.duplicates + vms.collect(&:uid_ems).duplicates).uniq.sort
        _log.info("#{log_header} Duplicate unique values found: #{dup_vms_uids.inspect}") unless dup_vms_uids.empty?

        invalids_found = false
        # Clear vms, so GC can clean them
        vms = nil

        ActiveRecord::Base.transaction do
          hashes.each do |h|
            # Backup keys that cannot be written directly to the database
            key_backup = backup_keys(h, remove_keys)

            h[:ems_id]                 = ems.id
            h[:flavor_id]              = key_backup.fetch_path(:flavor, :id)
            h[:availability_zone_id]   = key_backup.fetch_path(:availability_zone, :id)
            h[:cloud_network_id]       = key_backup.fetch_path(:cloud_network, :id)
            h[:cloud_subnet_id]        = key_backup.fetch_path(:cloud_subnet, :id)
            h[:cloud_tenant_id]        = key_backup.fetch_path(:cloud_tenant, :id)
            h[:cloud_tenant_ids]       = key_backup.fetch_path(:cloud_tenants).compact.map { |x| x[:id] } if key_backup.fetch_path(:cloud_tenants, 0, :id)

            begin
              raise MiqException::MiqIncompleteData if h[:invalid]

              # Find the Vm in the database with the current uid_ems.  In the event
              #   of duplicates, try to determine which one is correct.
              found = vms_by_uid_ems[h[:uid_ems]] || []

              if found.length > 1 || (found.length == 1 && found.first.ems_id)
                found_dups = found
                found = found_dups.select { |v| v.ems_id == h[:ems_id] && (v.ems_ref.nil? || v.ems_ref == h[:ems_ref]) }
                if found.empty?
                  found_dups = found_dups.select { |v| v.ems_id.nil? }
                  found = found_dups.select { |v| v.ems_ref == h[:ems_ref] }
                  found = found_dups if found.empty?
                end
              end
              found = found.first

              if found.nil?
                _log.info("#{log_header} Creating Vm [#{h[:name]}] location: [#{h[:location]}] storage id: [#{h[:storage_id]}] uid_ems: [#{h[:uid_ems]}] ems_ref: [#{h[:ems_ref]}]")

                # build a type-specific vm or template
                found = ems.instances.klass.new(h)
              else
                vms_by_uid_ems[h[:uid_ems]].delete(found)
                h.delete(:type)

                _log.info("#{log_header} Updating Vm [#{found.name}] id: [#{found.id}] location: [#{found.location}] storage id: [#{found.storage_id}] uid_ems: [#{found.uid_ems}] ems_ref: [#{h[:ems_ref]}]")
                found.update_attributes!(h)
                disconnects_index.delete(found)
              end

              # Set the raw power state
              found.raw_power_state = key_backup[:raw_power_state]

              #link_habtm(found, key_backup[:storages], :storages, Storage)
              #link_habtm(found, key_backup[:key_pairs], :key_pairs, ManageIQ::Providers::CloudManager::AuthKeyPair)
              save_child_inventory(found, key_backup, child_keys)

              found.save!
              h[:id] = found.id
            rescue => err
              # If a vm failed to process, mark it as invalid and log an error
              h[:invalid] = invalids_found = true
              name = h[:name] || h[:uid_ems] || h[:ems_ref]
              raise if EmsRefresh.debug_failures
              _log.error("#{log_header} Processing Vm: [#{name}] failed with error [#{err}]. Skipping Vm.")
            ensure
              restore_keys(h, remove_keys, key_backup)
            end
          end
        end

        # Handle genealogy link ups
        # TODO: can we use _object
        vm_ids = hashes.flat_map { |h| !h[:invalid] && h.has_key_path?(:parent_vm, :id) ? [h[:id], h.fetch_path(:parent_vm, :id)] : [] }.uniq
        unless vm_ids.empty?
          _log.info("#{log_header} Updating genealogy connections.")
          vms = Infra::VmOrTemplate.where(:id => vm_ids).index_by(&:id)
          hashes.each do |h|
            parent = vms[h.fetch_path(:parent_vm, :id)]
            child = vms[h[:id]]

            child.with_relationship_type('genealogy') { child.parent = parent } if parent && child
          end
        end

        disconnects = disconnects_index.values

        unless disconnects.empty?
          if invalids_found
            _log.warn("#{log_header} Since failures occurred, not disconnecting for Vms #{log_format_deletes(disconnects)}")
          elsif target.kind_of?(Host)
            # The disconnected VMs may actually just be moved to another Host.  We
            # don't have enough information to fully disconnect from the EMS, so
            # queue up a targeted refresh on that VM.
            $log.warn("#{log_header} Queueing targeted refresh, since we do not have enough " \
                      "information to fully disconnect Vms #{log_format_deletes(disconnects)}")
            EmsRefresh.queue_refresh(disconnects)

            $log.info("#{log_header} Partially disconnecting Vms #{log_format_deletes(disconnects)}")
            disconnects.each(&:disconnect_host)
          else
            _log.info("#{log_header} Disconnecting Vms #{log_format_deletes(disconnects)}")
            disconnects.each(&:disconnect_inv)
          end
        end
      end

      def save_flavors_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.flavors.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        hashes.each do |h|
          h[:cloud_tenant_ids] = (h.delete(:cloud_tenants) || []).compact.map { |x| x[:id] }.uniq
        end

        save_inventory_multi(ems.flavors, hashes, deletes, [:ems_ref])
        store_ids_for_new_records(ems.flavors, hashes, :ems_ref)
      end

      def save_availability_zones_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.availability_zones.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        save_inventory_multi(ems.availability_zones, hashes, deletes, [:ems_ref])
        store_ids_for_new_records(ems.availability_zones, hashes, :ems_ref)
      end

      def save_host_aggregates_inventory(ems, hashes, target = nil, disconnect = true)
        target ||= ems

        ems.host_aggregates.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        save_inventory_multi(ems.host_aggregates, hashes, deletes, [:ems_ref])
        store_ids_for_new_records(ems.host_aggregates, hashes, :ems_ref)
        # FIXME: what about hosts?
      end

      def save_cloud_tenants_inventory(ems, hashes, target = nil, disconnect = true)
        target ||= ems

        ems.cloud_tenants.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        save_inventory_multi(ems.cloud_tenants, hashes, deletes, [:ems_ref], nil, [:parent_id])
        store_ids_for_new_records(ems.cloud_tenants, hashes, :ems_ref)
      end

      def save_cloud_resource_quotas_inventory(ems, hashes, target = nil, disconnect = true)
        target ||= ems

        ems.cloud_resource_quotas.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        hashes.each do |h|
          h[:cloud_tenant_id] = h.fetch_path(:cloud_tenant, :id)
        end

        save_inventory_multi(ems.cloud_resource_quotas, hashes, deletes, [:ems_ref, :name], nil, :cloud_tenant)
        store_ids_for_new_records(ems.cloud_resource_quotas, hashes, [:ems_ref, :name])
      end

      def save_key_pairs_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.key_pairs.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        save_inventory_multi(ems.key_pairs, hashes, deletes, [:name])
        store_ids_for_new_records(ems.key_pairs, hashes, :name)
      end

      def save_cloud_volumes_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.cloud_volumes.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        hashes.each do |h|
          h[:ems_id]               = ems.id
          h[:cloud_tenant_id]      = h.fetch_path(:tenant, :id)
          h[:availability_zone_id] = h.fetch_path(:availability_zone, :id)
          # Defer setting :cloud_volume_snapshot_id until after snapshots are saved.
        end

        save_inventory_multi(ems.cloud_volumes, hashes, deletes, [:ems_ref], nil, [:tenant, :availability_zone, :base_snapshot])
        store_ids_for_new_records(ems.cloud_volumes, hashes, :ems_ref)
      end

      def save_cloud_volume_backups_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.cloud_volume_backups.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        hashes.each do |h|
          h[:ems_id]          = ems.id
          h[:cloud_volume_id] = h.fetch_path(:volume, :id)
          h[:availability_zone_id] = h.fetch_path(:availability_zone, :id)
        end

        save_inventory_multi(ems.cloud_volume_backups, hashes, deletes, [:ems_ref], nil,
                             [:tenant, :volume, :availability_zone])
        store_ids_for_new_records(ems.cloud_volume_backups, hashes, :ems_ref)
      end

      def save_cloud_volume_snapshots_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.cloud_volume_snapshots.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        hashes.each do |h|
          h[:ems_id]          = ems.id
          h[:cloud_tenant_id] = h.fetch_path(:tenant, :id)
          h[:cloud_volume_id] = h.fetch_path(:volume, :id)
        end

        save_inventory_multi(ems.cloud_volume_snapshots, hashes, deletes, [:ems_ref], nil, [:tenant, :volume])
        store_ids_for_new_records(ems.cloud_volume_snapshots, hashes, :ems_ref)
      end

      def link_volumes_to_base_snapshots(hashes)
        base_snapshot_to_volume = hashes.each_with_object({}) do |h, bsh|
          next unless (base_snapshot = h[:base_snapshot])
          (bsh[base_snapshot[:id]] ||= []) << h[:id]
        end

        base_snapshot_to_volume.each do |bsid, volids|
          CloudVolume.where(:id => volids).update_all(:cloud_volume_snapshot_id => bsid)
        end
      end

      def link_parents_to_cloud_tenant(hashes)
        mapped_ids = hashes.each_with_object({}) do |cloud_tenant, mapped_ids_hash|
          ems_ref_parent_id = cloud_tenant[:parent_id]
          next if ems_ref_parent_id.nil?

          parent_cloud_tenant = hashes.detect { |x| x[:ems_ref] == ems_ref_parent_id }
          next if parent_cloud_tenant.nil?

          (mapped_ids_hash[parent_cloud_tenant[:id]] ||= []) << cloud_tenant[:id]
        end

        mapped_ids.each do |parent_id, ids|
          CloudTenant.where(:id => ids).update_all(:parent_id => parent_id)
        end
      end

      def save_cloud_object_store_containers_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.cloud_object_store_containers.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        hashes.each do |h|
          h[:ems_id]          = ems.id
          h[:cloud_tenant_id] = h.fetch_path(:tenant, :id)
        end

        save_inventory_multi(ems.cloud_object_store_containers, hashes, deletes, [:ems_ref], nil, :tenant)
        store_ids_for_new_records(ems.cloud_object_store_containers, hashes, :ems_ref)
      end

      def save_cloud_object_store_objects_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.cloud_object_store_objects.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        hashes.each do |h|
          h[:ems_id]                          = ems.id
          h[:cloud_tenant_id]                 = h.fetch_path(:tenant, :id)
          h[:cloud_object_store_container_id] = h.fetch_path(:container, :id)
        end

        save_inventory_multi(ems.cloud_object_store_objects, hashes, deletes, [:ems_ref], nil, [:tenant, :container])
        store_ids_for_new_records(ems.cloud_object_store_objects, hashes, :ems_ref)
      end

      def save_resource_groups_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.resource_groups.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        save_inventory_multi(ems.resource_groups, hashes, deletes, [:ems_ref])
        store_ids_for_new_records(ems.resource_groups, hashes, :ems_ref)
      end

      def save_cloud_services_inventory(ems, hashes, target = nil, disconnect = true)
        target = ems if target.nil?

        ems.cloud_services.reset
        deletes = determine_deletes_using_association(ems, target, disconnect)

        save_inventory_multi(ems.cloud_services, hashes, deletes, [:ems_ref])
        store_ids_for_new_records(ems.cloud_services, hashes, :ems_ref)
      end
    end
  end
end
