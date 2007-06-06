# ident for core dump
CVS_ID = '$Id$'

LIB_DIR = 'lib'
TEST_DIR = 'test'
RDOC_DIR = 'api'
RDOC_MAIN = 'higgs.rb'

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
    sh 'rdoc', '-a', '-i', '..', '-o', "../#{RDOC_DIR}", '-m', RDOC_MAIN
  }
end

require 'rake/gempackagetask'
spec = Gem::Specification.new{|s|
  s.name = 'higgs'
  s.version = '0.0.1'
  s.summary = 'transactional storage'
  s.author = 'TOKI Yoshinori'
  s.email = 'toki@freedom.ne.jp'
  s.files = Dir['{bin,lib,test,misc}/**/*.rb'] << 'ChangeLog'
  s.test_files = [ 'test/run.rb' ]
  s.has_rdoc = false
}
Rake::GemPackageTask.new(spec) do |pkg|
  pkg.need_zip = true
  pkg.need_tar = true
end

task :clean => [ :clobber_package ] do
  rm_rf RDOC_DIR
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
