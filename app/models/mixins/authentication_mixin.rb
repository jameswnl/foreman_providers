module AuthenticationMixin
  extend ActiveSupport::Concern

  included do
    has_many :authentications, :as => :resource, :dependent => :destroy, :autosave => true
  end

  def supported_auth_attributes
    %w(userid password)
  end

  def default_authentication_type
    :default
  end

  def authentication_userid_passwords
    authentications.select { |a| a.kind_of?(Providers::AuthUseridPassword) }
  end

  def authentication_for_providers
    authentications.where.not(:authtype => nil)
  end

  def authentication_for_summary
    summary = []
    authentication_for_providers.each do |a|
      summary << {
        :authtype       => a.authtype,
        :status         => a.status,
        :status_details => a.status_details
      }
    end
    summary
  end

  def has_authentication_type?(type)
    authentication_types.include?(type)
  end

  def authentication_userid(type = nil)
    authentication_component(type, :userid)
  end

  def authentication_password(type = nil)
    authentication_component(type, :password)
  end

  def required_credential_fields(_type)
    [:userid]
  end

  def has_credentials?(type = nil)
    required_credential_fields(type).all? { |field| authentication_component(type, field) }
  end

  def missing_credentials?(type = nil)
    !has_credentials?(type)
  end

  def authentication_status
    ordered_auths = authentication_for_providers.sort_by(&:status_severity)
    ordered_auths.last.try(:status) || "None"
  end

  def authentication_status_ok?(type = nil)
    authentication_best_fit(type).try(:status) == "Valid"
  end

  def auth_user_pwd(type = nil)
    cred = authentication_best_fit(type)
    return nil if cred.nil? || cred.userid.blank?
    [cred.userid, cred.password]
  end

  def update_authentication(data, options = {})
    return if data.blank?

    options.reverse_merge!(:save => true)

    @orig_credentials ||= auth_user_pwd || "none"

    # Invoke before callback
    before_update_authentication if self.respond_to?(:before_update_authentication) && options[:save]

    data.each_pair do |type, value|
      cred = authentication_type(type)
      current = {:new => nil, :old => nil}

      unless value.key?(:userid) && value[:userid].blank?
        current[:new] = {:user => value[:userid], :password => value[:password]}
      end
      current[:old] = {:user => cred.userid, :password => cred.password} if cred

      # Raise an error if required fields are blank
      Array(options[:required]).each { |field| raise(ArgumentError, "#{field} is required") if value[field].blank? }

      # If old and new are the same then there is nothing to do
      next if current[:old] == current[:new]

      # Check if it is a delete
      if value.key?(:userid) && value[:userid].blank?
        current[:new] = nil
        next if options[:save] == false
        authentication_delete(type)
        next
      end

      # Update or create
      if cred.nil?
        cred = authentications.build(:name => "#{self.class.name} #{name}", :authtype => type.to_s,
                                     :type => "Providers::AuthUseridPassword")
      end

      cred.userid = value[:userid]
      cred.password = value[:password]

      cred.save if options[:save] && id
    end

    # Invoke callback
    after_update_authentication if self.respond_to?(:after_update_authentication) && options[:save]
    @orig_credentials = nil if options[:save]
  end

  def credentials_changed?
    @orig_credentials ||= auth_user_pwd || "none"
    new_credentials = auth_user_pwd || "none"
    @orig_credentials != new_credentials
  end

  def authentication_type(type)
    return nil if type.nil?
    available_authentications.detect do |a|
      a.authentication_type.to_s == type.to_s
    end
  end

  def authentication_check_types(*args)
    options = args.extract_options!

    # Let the individual classes determine what authentication(s) need to be checked
    types = authentications_to_validate if respond_to?(:authentications_to_validate)
    types = args.first                  if types.blank?
    types = [nil]                       if types.blank?
    Array(types).each do |t|
      success = authentication_check(t, options.except(:attempt)).first
    end
  end

  # Returns [boolean check_result, string details]
  # check_result is true if and only if:
  #   * the system is reachable
  #   * AND we have the required authentication information
  #   * AND we successfully connected using the authentication
  #
  # details is a UI friendly message
  #
  # By default, the authentication's status is updated by the
  # validation_successful or validation_failed callbacks.
  #
  # An optional :save => false can be passed to bypass these callbacks.
  #
  # TODO: :valid, :incomplete, and friends shouldn't be littered in here and authentication
  def authentication_check(*args)
    options         = args.last.kind_of?(Hash) ? args.last : {}
    save            = options.fetch(:save, true)
    auth            = authentication_best_fit(args.first)
    type            = args.first || auth.try(:authtype)
    status, details = authentication_check_no_validation(type, options)

    if auth && save
      status == :valid ? auth.validation_successful : auth.validation_failed(status, details)
    end

    return status == :valid, details.truncate(20_000)
  end

  def default_authentication
    authentication_type(default_authentication_type)
  end

  private

  def authentication_check_no_validation(type, options)
    header  = "type: [#{type.inspect}] for [#{id}] [#{name}]"
    status, details =
      if self.missing_credentials?(type)
        [:incomplete, "Missing credentials"]
      else
        begin
          verify_credentials(type, options) ? [:valid, ""] : [:invalid, "Unknown reason"]
        rescue => err
          [:error, err]
        end
      end

    details &&= details.to_s

    #_log.warn("#{header} Validation failed: #{status}, #{details.truncate(200)}") unless status == :valid
    return status, details
  end

  def authentication_best_fit(type = nil)
    # Look for the supplied type and if that is not found return the default credentials
    authentication_type(type) || authentication_type(default_authentication_type)
  end

  def authentication_component(type, method)
    cred = authentication_best_fit(type)
    return nil if cred.nil?

    value = cred.public_send(method)
    value.blank? ? nil : value
  end

  def available_authentications
    authentication_userid_passwords
  end

  def authentication_types
    available_authentications.collect(&:authentication_type).uniq
  end

  def authentication_delete(type)
    a = authentication_type(type)
    authentications.destroy(a) unless a.nil?
    a
  end
end
