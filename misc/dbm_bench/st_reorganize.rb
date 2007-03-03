#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

$: << File.join(File.dirname($0), '..', '..', 'lib')

require 'benchmark'
require 'higgs/storage'

name = File.join(File.dirname($0), 'foo')
st = Higgs::Storage.new(name)

Benchmark.bm do |x|
  x.report('st reorganize') {
    st.reorganize
  }
end
