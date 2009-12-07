# -*- coding: utf-8 -*-

require 'lib/higgs/version'
require 'rake/gempackagetask'
require 'rbconfig'

RbConfig::CONFIG['RUBY_INSTALL_NAME'] =~ /(.*)ruby(.*)/i or raise 'not found RUBY_INSTALL_NAME'
prefix = $1
suffix = $2

RAKE_CMD = "#{prefix}rake#{suffix}"
RDOC_CMD = "#{prefix}rdoc#{suffix}"
GEM_CMD = "#{prefix}gem#{suffix}"

LIB_DIR = 'lib'
TEST_DIR = 'test'

def cd_v(dir)
  cd(dir, :verbose => true) {
    yield
  }
end

task :default

desc 'unit-test.'
task :test do
  cd_v TEST_DIR do
    sh RAKE_CMD
  end
end

rdoc_dir = 'api'
rdoc_opts = [ '-SNa', '-m', 'Higgs', '-t', 'pure ruby transactional storage compatible with unix TAR format' ]

desc 'make document.'
task :rdoc do
  sh RDOC_CMD, *rdoc_opts, '-o', rdoc_dir, 'lib'
end

desc 'clean document.'
task :rdoc_clean do
  rm_rf rdoc_dir
end

desc 'upload document to higgs.rubyforge.org.'
task :rdoc_upload => [ :rdoc_clean, :rdoc ] do
  sh 'scp', '-pr', rdoc_dir, 'rubyforge.org:/var/www/gforge-projects/higgs/.'
end

spec = Gem::Specification.new{|s|
  s.name = 'higgs'
  s.version = Higgs::VERSION
  s.summary = 'pure ruby transactional storage compatible with unix TAR format'
  s.author = 'TOKI Yoshinori'
  s.email = 'toki@freedom.ne.jp'
  s.homepage = 'http://higgs.rubyforge.org/'
  s.rubyforge_project = 'higgs'
  s.executables  = %w[
    higgs_apply_jlog
    higgs_backup
    higgs_dump_index
    higgs_dump_jlog
    higgs_ping
    higgs_verify
  ]
  s.files =
    Dir['{lib,misc,sample,test}/**/{Rakefile,.strc,*.rb,*.yml}'] +
    %w[ ChangeLog README Rakefile LICENSE ]
  s.test_files = %w[ test/run.rb ]
  s.has_rdoc = true
  s.rdoc_options = rdoc_opts
}
Rake::GemPackageTask.new(spec) do |pkg|
  pkg.need_zip = true
  pkg.need_tar_gz = true
end

desc 'install gem.'
task :gem_install => [ :gem ] do
  sh GEM_CMD, 'install', "pkg/higgs-#{Higgs::VERSION}.gem"
end

desc 'clean garbage files'
task :clean => [ :rdoc_clean, :clobber_package ] do
  cd_v 'misc/dbm_bench' do
    sh RAKE_CMD, 'clean'
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
