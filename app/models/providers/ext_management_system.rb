module Providers
  class ExtManagementSystem < ApplicationRecord
    has_many :endpoints, :as => :resource, :dependent => :destroy, :autosave => true
    has_many :hosts, :foreign_key => "ems_id", :class_name => "Providers::Infra::Host", :dependent => :nullify
    has_many :vms,   :foreign_key => "ems_id", :class_name => "Providers::Infra::Vm", :dependent => :nullify

    validates :name,     :presence => true, :uniqueness => {:scope => [:tenant_id]}
    validates :hostname, :presence => true, :if => :hostname_required?
    validate  :hostname_uniqueness_valid?, :if => :hostname_required?

    serialize :options

    def hostname_uniqueness_valid?
      return unless hostname_required?
      return unless hostname.present? # Presence is checked elsewhere
      # check uniqueness per provider type

      existing_hostnames = (self.class.all - [self]).map(&:hostname).compact.map(&:downcase)

      errors.add(:hostname, N_("has to be unique per provider type")) if existing_hostnames.include?(hostname.downcase)
    end

    include NewWithTypeStiMixin
    include AuthenticationMixin

    delegate :ipaddress,
             :ipaddress=,
             :hostname,
             :hostname=,
             :port,
             :port=,
             :security_protocol,
             :security_protocol=,
             :certificate_authority,
             :certificate_authority=,
             :to => :default_endpoint,
             :allow_nil => true

    def self.with_ipaddress(ipaddress)
      joins(:endpoints).where(:endpoints => {:ipaddress => ipaddress})
    end

    def self.with_hostname(hostname)
      joins(:endpoints).where(:endpoints => {:hostname => hostname})
    end

    def self.with_role(role)
      joins(:endpoints).where(:endpoints => {:role => role})
    end

    def self.with_port(port)
      joins(:endpoints).where(:endpoints => {:port => port})
    end

    def default_endpoint
      default = endpoints.detect { |e| e.role == "default" }
      default || endpoints.build(:role => "default", :verify_ssl => OpenSSL::SSL::VERIFY_PEER)
    end

    def hostnames
      hostnames ||= endpoints.map(&:hostname)
      hostnames
    end

    def authentication_check_role
      'ems_operations'
    end

    def self.hostname_required?
      true
    end
    delegate :hostname_required?, :to => :class

    def last_refresh_status
      if last_refresh_date
        last_refresh_error ? "error" : "success"
      else
        "never"
      end
    end

    def disable!
      _log.info("Disabling EMS [#{name}] id [#{id}].")
      update!(:enabled => false)
    end

    def enable!
      _log.info("Enabling EMS [#{name}] id [#{id}].")
      update!(:enabled => true)
    end

    def build_connection(options = {})
      build_endpoint_by_role(options[:endpoint])
      build_authentication_by_role(options[:authentication])
    end
  end
end
