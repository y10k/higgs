# for ident(1)
CVS_ID = '$Id$'

LIB_DIR = 'lib'
TEST_DIR = 'test'
RDOC_DIR = 'api'
RDOC_MAIN = 'Higgs'

def cd_v(dir)
  cd(dir, :verbose => true) {
    yield
  }
end

task :default

task :test do
  cd_v(TEST_DIR) {
    sh 'rake'
  }
end

task :rdoc do
  cd_v(LIB_DIR) {
    sh 'rdoc', '-SNa', '-i', '..', '-o', "../#{RDOC_DIR}", '-m', RDOC_MAIN
  }
end

require 'rake/gempackagetask'
require 'lib/higgs/version'
spec = Gem::Specification.new{|s|
  s.name = 'higgs'
  s.version = Higgs::VERSION
  s.summary = 'pure ruby transactional storage compatible with unix TAR format'
  s.author = 'TOKI Yoshinori'
  s.email = 'toki@freedom.ne.jp'
  s.executables << 'higgs_dump_index' << 'higgs_dump_jlog' << 'higgs_verify' << 'higgs_backup'
  s.files = Dir['{lib,test,misc,sample}/**/*.rb'] << 'ChangeLog' << 'LICENSE'
  s.test_files = [ 'test/run.rb' ]
  s.has_rdoc = true
}
Rake::GemPackageTask.new(spec) do |pkg|
  pkg.need_zip = true
  pkg.need_tar_gz = true
end

task :gem_install => [ :gem ] do
  sh 'gem', 'install', "pkg/higgs-#{Higgs::VERSION}.gem"
end

task :clean => [ :clobber_package ] do
  rm_rf RDOC_DIR
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
