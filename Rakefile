require 'rake/extensiontask'

Rake::ExtensionTask.new('nvmkv_driver')

PKG_FILES = FileList[
  'ext/nvmkv_driver/nvmkv_driver.c',
  'ext/nvmkv_driver/extconf.rb',
  'lib/nvmkv_driver.rb',
  'lib/nvmkv.rb',
#  'spec/*'
]

spec = Gem::Specification.new do |s|
  s.name = 'ruby-nvmkv'
  s.version = '0.1'
  s.summary ='OpenNVM NVMKV interface'
  s.description = 'ruby-nvmkv provides the interface for OpenNVM for Ruby'
  s.licenses = ['The BSD License']
  s.authors = ['Takeshi HASEGAWA']
  s.email = 'hasegaw@gmail.com' 
  s.homepage = 'http://opennvm.github.io'

  s.platform = Gem::Platform::CURRENT
  s.extensions = FileList["ext/**/extconf.rb"]
  s.files = PKG_FILES.to_a
  #s.test_globs = ['test/test_*.rb']
  #	s.readme_file = 'README.md'
  #	s.history_file = 'History.txt'
  s.require_paths = [ 'lib', 'ext' ]
end

# add your default gem packing task
Gem::PackageTask.new(spec) do |pkg|
  pkg.gem_spec = spec
end
