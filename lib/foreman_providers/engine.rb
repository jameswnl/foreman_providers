module ForemanProviders
  class Engine < ::Rails::Engine
    engine_name 'foreman_providers'

    config.autoload_paths += Dir["#{config.root}/app/controllers/concerns"]
    config.autoload_paths += Dir["#{config.root}/app/helpers/concerns"]
    config.autoload_paths += Dir["#{config.root}/app/models/concerns"]
    config.autoload_paths += Dir["#{config.root}/app/models/mixins"]
    config.autoload_paths += Dir["#{config.root}/app/overrides"]

    # Add any db migrations
    initializer 'foreman_providers.load_app_instance_data' do |app|
      ForemanProviders::Engine.paths['db/migrate'].existent.each do |path|
        app.config.paths['db/migrate'] << path
      end
    end

    initializer 'foreman_providers.register_plugin', :before => :finisher_hook do |app|
      require 'extensions/descendant_loader'

      # make sure STI models are recognized
      DescendantLoader.instance.descendants_paths << config.root.join('app')

      Foreman::Plugin.register :foreman_providers do
        requires_foreman '>= 1.4'

        # Add permissions
        security_block :foreman_providers do
          permission :view_foreman_providers, :'foreman_providers/hosts' => [:new_action]
        end

        # Add a new role called 'Discovery' if it doesn't exist
        role 'ForemanProviders', [:view_foreman_providers]

        # add menu entry
        menu :top_menu, :template,
             url_hash: { controller: :'foreman_providers/hosts', action: :new_action },
             caption: 'ForemanProviders',
             parent: :hosts_menu,
             after: :hosts

        # add dashboard widget
        widget 'foreman_providers_widget', name: N_('Foreman plugin template widget'), sizex: 4, sizey: 1
      end
    end

    # Precompile any JS or CSS files under app/assets/
    # If requiring files from each other, list them explicitly here to avoid precompiling the same
    # content twice.
    assets_to_precompile =
      Dir.chdir(root) do
        Dir['app/assets/javascripts/**/*', 'app/assets/stylesheets/**/*'].map do |f|
          f.split(File::SEPARATOR, 4).last
        end
      end
    initializer 'foreman_providers.assets.precompile' do |app|
      app.config.assets.precompile += assets_to_precompile
    end
    initializer 'foreman_providers.configure_assets', group: :assets do
      SETTINGS[:foreman_providers] = { assets: { precompile: assets_to_precompile } }
    end

    # Include concerns in this config.to_prepare block
    config.to_prepare do
      begin
        Foreman::Model::Ovirt.include(ForemanProviders::ComputeResource)
      rescue => e
        Rails.logger.warn "ForemanProviders: skipping engine hook (#{e})"
      end
    end

    rake_tasks do
      Rake::Task['db:seed'].enhance do
        ForemanProviders::Engine.load_seed
      end
    end

    initializer 'foreman_providers.register_gettext', after: :load_config_initializers do |_app|
      locale_dir = File.join(File.expand_path('../../..', __FILE__), 'locale')
      locale_domain = 'foreman_providers'
      Foreman::Gettext::Support.add_text_domain locale_domain, locale_dir
    end
  end
end
