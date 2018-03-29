# ForemanProviders

Adds ManageIQ Providers and Inventory to Foreman

## Installation

Clone the provider plugins:

```bash
git clone https://github.com/agrare/foreman_providers.git
git clone https://github.com/agrare/foreman_providers_infra.git
git clone https://github.com/agrare/foreman_providers_ovirt.git
```

Add the provider plugins to your Foreman bundler.d/ directory:

```bash
echo 'gem "ovirt" # require the ovirt gem early to workaround issues with rbovirt
gem "foreman_providers",      :path => "../foreman_providers"
gem "foreman_providers_infra", :path => "../foreman_providers_infra"
gem "foreman_providers_ovirt", :path => "../foreman_providers_ovirt"' > bundler.d/provider.rb
```

Update your foreman gems and database

```bash
bundle update
bin/rails db:migrate
```

## Usage

Add a new Ovirt ComputeResource and a new Provider will be automatically created for you

Refresh your provider inventory

```bash
bin/rails runner 'Providers::Ovirt::Manager.first.refresh'
```

This should automatically create managed hosts from the Ovirt Hosts and VMs in your inventory.

Additionally there may be more VMs that aren't listed if they don't have an IP address that was detected.

These VMs are in the `Providers::Infra::Vm` model, templates are in the `Providers::Infra::Template` model, and hosts in (wait for it) `Providers::Infra::Host`.

## TODO

*Todo list here*

## Contributing

Fork and send a Pull Request. Thanks!

## Copyright

Copyright (c) 2018 Red Hat, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

