require File.expand_path('../lib/foreman_providers/version', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'foreman_providers'
  s.version     = ForemanProviders::VERSION
  s.license     = 'GPL-3.0'
  s.authors     = ['Adam Grare', 'Ladislav Smola', 'James Wong']
  s.email       = ['agrare@redhat.com', 'lsmola@redhat.com', 'jwong@redhat.com']
  s.homepage    = 'https://github.com/agrare/foreman_providers'
  s.summary     = 'Providers plugin for Foreman.'
  # also update locale/gemspec.rb
  s.description = 'Providers plugin for Foreman.'

  s.files = Dir['{app,config,db,lib,locale}/**/*'] + ['LICENSE', 'Rakefile', 'README.md']
  s.test_files = Dir['test/**/*']

  s.add_runtime_dependency     'ripper_ruby_parser', '~>1.2.0'
  s.add_development_dependency 'rubocop'
  s.add_development_dependency 'rdoc'
end
