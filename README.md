# nvmkv-ruby

nvmkv-ruby is binding library for Ruby programming language.
This library provides the access to NVMKV library in OpenNVM.

nvmkv-ruby requires NVMKV library. nvmkv-ruby-0.1 is tested with NVMKV v1.0.0
which can be obtained from http://opennvm.github.io.

# Installation

Before installing nvmkv-ruby, you should install softwares below.

- ioMemory VSL 3.3.3 (for Fusion-io devices)
- NVMKV library v1.0.0

You would want to get Ruby 1.9.3 from Red Hat Software Collections (SCL).

```
  # Install Ruby 1.9.3 from SCL
  cd /etc/yum.repos.d/
  wget http://people.redhat.com/bkabrda/scl_ruby193.repo
  yum install ruby193-ruby
  yum install ruby193-ruby-devel
  yum install ruby193-rubygem-rake

  # Update gem, Install rake on Ruby 1.9.3 environment
  scl enable ruby193 bash     # You will have the child shell
  ruby -v                     # => ruby 1.9.3pXXX ....
  gem update --system
  gem install rake-compiler
  gem install rake

  # Install NVMKV bindings onto Ruby 1.9.3 environment
  cd /path/to/nvmkv-ruby
  rake gem
  gem install --local pkg/ruby-nvmkv-0.1-x86_64-linux.gem  
  exit # leave from the child shell
```

# Code Example

```ruby
  # Load NVMKV
  require 'nvmkv'
  include NVMKV

  # Create (or open) NVMKV store on /dev/fioa
  store = NVMKVStore.new
  store.open('/dev/fioa')

  # Clear any data on the store
  store.delete_all

  # Create a pool tagged 'hello'
  pool = store.new_pool('hello')

  pool.put('ruby', 'hello world')
  p pool.get('ruby').value # => 'hello world'

  store.close
```

To run this script on Ruby 1.9.3 environment:

```
  # ruby193-ruby test.rb 
  "hello world"
```

# Performance

nvmkv-ruby unlocks Giant VM Lock (aka GVL). Applications can get optimal performance by concurrent NVMKV accesss,
if (1) running Ruby 1.9 or later, (2) Ruby is configured with libpthread, and (3) Ruby script run concurrently using threads.

# See Also

* OpenNVM Project <http://opennvm.github.io/>
* NVMKV API <http://opennvm.github.io/nvmkv-documents/>

# License

nvmkv-ruby is available for use under the following license, commonly known
as the 3-clause (or "modified") BSD license.
