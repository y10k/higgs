# -*- coding: utf-8 -*-

require 'lib/higgs/version'
require 'rake/gempackagetask'

LIB_DIR = 'lib'
TEST_DIR = 'test'

def cd_v(dir)
  cd(dir, :verbose => true) {
    yield
  }
end

task :default

task :test do
  cd_v TEST_DIR do
    sh 'rake'
  end
end

rdoc_dir = 'api'
rdoc_opts = [ '-SNa', '-m', 'Higgs', '-t', 'pure ruby transactional storage compatible with unix TAR format' ]

task :rdoc do
  sh 'rdoc', *rdoc_opts, '-o', rdoc_dir, 'lib'
end

task :rdoc_clean do
  rm_rf rdoc_dir
end

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
    %w[ ChangeLog README Rakefile lib/LICENSE mkrdoc.rb rdoc.yml ]
  s.test_files = %w[ test/run.rb ]
  s.has_rdoc = true
  s.rdoc_options = rdoc_opts
}
Rake::GemPackageTask.new(spec) do |pkg|
  pkg.need_zip = true
  pkg.need_tar_gz = true
end

task :gem_install => [ :gem ] do
  sh 'gem', 'install', "pkg/higgs-#{Higgs::VERSION}.gem"
end

task :clean => [ :rdoc_clean, :clobber_package ] do
  cd_v 'misc/dbm_bench' do
    sh 'rake', 'clean'
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
