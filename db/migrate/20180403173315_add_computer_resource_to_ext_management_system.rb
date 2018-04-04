class AddComputerResourceToExtManagementSystem < ActiveRecord::Migration[5.1]
  def change
    add_column :providers_ext_management_systems, :compute_resource_id, :integer
  end
end
