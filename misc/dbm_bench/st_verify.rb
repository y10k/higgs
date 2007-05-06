#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

$: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/storage'

puts $0

name = File.join(File.dirname($0), 'foo')
st = Higgs::Storage.new(name, :read_only => true)
begin
  Benchmark.bm do |x|
    x.report('st verify') {
      st.verify
    }
  end
  print "\n"
ensure
  st.shutdown
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
