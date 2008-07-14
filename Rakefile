# for ident(1)
CVS_ID = '$Id$'

require 'lib/higgs/version'
require 'rake/gempackagetask'
require 'yaml'

LIB_DIR = 'lib'
TEST_DIR = 'test'
RDOC_DIR = 'api'

def cd_v(dir)
  cd(dir, :verbose => true) {
    yield
  }
end

def load_rdoc_opts
  YAML.load(IO.read('rdoc.yml'))
end

task :default

task :test do
  cd_v TEST_DIR do
    sh 'rake'
  end
end

task :rdoc do
  rdoc_opts = load_rdoc_opts
  sh 'rdoc', *(rdoc_opts['CommonOptions'] + rdoc_opts['CommandLineOptions']).flatten
end

task :rdoc_clean do
  rm_rf RDOC_DIR
end

task :rdoc_upload => [ :rdoc_clean, :rdoc ] do
  sh 'scp', '-pr', RDOC_DIR, 'rubyforge.org:/var/www/gforge-projects/higgs/.'
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
    %w[ ChangeLog LICENSE README ]
  s.test_files = %w[ test/run.rb ]
  s.has_rdoc = true
  s.rdoc_options = load_rdoc_opts['CommonOptions'].flatten
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
